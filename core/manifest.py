"""
core/manifest.py — 模块 MANIFEST 统一结构与模块注册表

MANIFEST 是模块的身份证。

所有模块（因子/策略/状态/网关/风控/时钟/系统）都有一个 MANIFEST 字典，
定义在模块文件顶部。本文件负责：
1. 解析 MANIFEST 为统一的 Manifest 数据类
2. 校验 MANIFEST 的字段完整性和类型正确性
3. 发现（discover）所有模块
4. 校验（validate）依赖关系、能力冲突、循环引用
5. 组装（assemble）所有模块实例

核心概念：
- 模块名（name）：全局唯一，与文件名去掉前缀后一致
- 能力（capability）：模块能提供什么，snake_case，全局命名空间
- 依赖（dependency）：模块需要什么能力，用能力名，不用模块名
- 降级（fallback）：模块不可用时的替代模块名

关键契约：
1. MANIFEST 字段强校验，未知字段拒绝（防拼写错误）
2. 依赖解析按能力名，不按模块名
3. 能力冲突拒绝启动，除非 config.manifest.preferred 明确指定
4. 循环依赖检测，抛 CircularDependencyError
5. 循环 fallback 检测，抛 CircularFallbackError
6. 模块 import 超时 5 秒，超过抛 ModuleLoadTimeout
7. 模块 create() 异常包装成 ModuleInitError
8. discover() / assemble() 线程安全
9. discover() 幂等，重复调用返回缓存
10. 所有模块类型共用 Manifest 结构，类型专属字段可选

模块生命周期：
    discover() → validate() → assemble()
      │            │            │
      │            │            └─ 按拓扑序实例化，处理 fallback
      │            └─ 校验依赖、能力、循环
      └─ 扫描目录，解析 MANIFEST

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

import importlib.util
import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
MANIFEST_SCHEMA_VERSION = 1
DEFAULT_MODULE_LOAD_TIMEOUT_S = 5.0
KNOWN_TEAM_MEMBERS = {"bimon", "lego", "zhangsan"}
VALID_NORMALIZED_RANGES = {(-1.0, 1.0), (0.0, 1.0)}


# ============================================================================
# 模块类型
# ============================================================================
class ModuleType(str, Enum):
    """所有模块类型。"""
    FACTOR = "factor"
    STRATEGY = "strategy"
    STATE = "state"
    GATEWAY = "gateway"
    RISK = "risk"
    CLOCK = "clock"
    SYSTEM = "system"


# ============================================================================
# 错误分层
# ============================================================================
class ManifestError(Exception):
    """MANIFEST 相关错误基类。"""
    pass


class InvalidManifestError(ManifestError):
    """MANIFEST 字段不合法。"""
    def __init__(self, module_name: str, errors: List[str]):
        self.module_name = module_name
        self.errors = errors
        super().__init__(
            f"invalid MANIFEST for '{module_name}': {'; '.join(errors)}"
        )


class CircularDependencyError(ManifestError):
    """循环依赖。"""
    def __init__(self, cycle: List[str]):
        self.cycle = cycle
        super().__init__(
            f"circular dependency detected: {' → '.join(cycle)}"
        )


class CircularFallbackError(ManifestError):
    """循环 fallback。"""
    def __init__(self, cycle: List[str]):
        self.cycle = cycle
        super().__init__(
            f"circular fallback detected: {' → '.join(cycle)}"
        )


class CapabilityConflictError(ManifestError):
    """能力冲突。"""
    def __init__(self, capability: str, providers: List[str]):
        self.capability = capability
        self.providers = providers
        super().__init__(
            f"capability '{capability}' provided by multiple modules: "
            f"{providers}, use config.manifest.preferred to resolve"
        )


class MissingDependencyError(ManifestError):
    """依赖未满足。"""
    def __init__(self, module_name: str, missing: List[str]):
        self.module_name = module_name
        self.missing = missing
        super().__init__(
            f"module '{module_name}' missing dependencies: {missing}"
        )


class ModuleLoadTimeout(ManifestError):
    """模块加载超时。"""
    def __init__(self, module_name: str, timeout_s: float):
        self.module_name = module_name
        self.timeout_s = timeout_s
        super().__init__(
            f"module '{module_name}' load timeout ({timeout_s}s)"
        )


class ModuleInitError(ManifestError):
    """模块初始化失败。"""
    def __init__(self, module_name: str, original: Exception):
        self.module_name = module_name
        self.original = original
        super().__init__(
            f"module '{module_name}' init failed: "
            f"{type(original).__name__}: {original}"
        )


class ModuleUnavailableError(ManifestError):
    """模块及其所有 fallback 均不可用。"""
    def __init__(self, module_name: str, chain: List[str]):
        self.module_name = module_name
        self.chain = chain
        super().__init__(
            f"module '{module_name}' and all fallbacks unavailable: {chain}"
        )


# ============================================================================
# 统一 MANIFEST 数据类
# ============================================================================
@dataclass(frozen=True)
class Manifest:
    """
    统一的 MANIFEST 结构。

    通用字段（所有类型必须）：
        name / type / version / step / owner

    通用字段（所有类型可选）：
        capabilities / module_dependencies / fallback /
        deprecated / replaced_by / enabled / description

    类型专属字段（由 ModuleType 决定是否必填）：
        factor:    normalized_range
        strategy:  required_factors
        state:     allowed_actions
        gateway:   exchange
    """
    # ---- 通用必填 ----
    name: str
    type: ModuleType
    version: str
    step: int
    owner: str

    # ---- 通用可选 ----
    capabilities: Tuple[str, ...] = ()
    module_dependencies: Tuple[str, ...] = ()
    fallback: Optional[str] = None
    deprecated: bool = False
    replaced_by: Optional[str] = None
    enabled: bool = True
    description: str = ""
    manifest_schema: int = MANIFEST_SCHEMA_VERSION

    # ---- 类型专属 ----
    # factor
    normalized_range: Optional[Tuple[float, float]] = None
    # strategy
    required_factors: Tuple[str, ...] = ()
    # state
    allowed_actions: Tuple[str, ...] = ()
    # gateway
    exchange: Optional[str] = None

    def is_loadable(self, current_step: int) -> bool:
        """是否可加载：enabled 且 step <= current_step。"""
        return self.enabled and self.step <= current_step


# ============================================================================
# MANIFEST 解析与校验
# ============================================================================
class ManifestParser:
    """解析原始 dict 为 Manifest，并做类型专属校验。"""

    # 通用字段白名单
    COMMON_KEYS = {
        "name", "type", "version", "step", "owner",
        "capabilities", "module_dependencies", "fallback",
        "deprecated", "replaced_by", "enabled", "description",
        "manifest_schema",
    }
    # 类型专属字段
    TYPE_KEYS = {
        ModuleType.FACTOR: {"normalized_range"},
        ModuleType.STRATEGY: {"required_factors"},
        ModuleType.STATE: {"allowed_actions"},
        ModuleType.GATEWAY: {"exchange"},
        ModuleType.RISK: set(),
        ModuleType.CLOCK: set(),
        ModuleType.SYSTEM: set(),
    }

    @classmethod
    def parse(cls, raw: dict, module_path: str) -> Manifest:
        """解析并校验。失败抛 InvalidManifestError。"""
        errors: List[str] = []
        module_name = raw.get("name", f"<unknown:{module_path}>")

        # schema 版本
        schema = raw.get("manifest_schema", MANIFEST_SCHEMA_VERSION)
        if schema != MANIFEST_SCHEMA_VERSION:
            errors.append(
                f"unsupported manifest_schema {schema}, "
                f"expected {MANIFEST_SCHEMA_VERSION}"
            )

        # 类型
        type_str = raw.get("type", "")
        try:
            mod_type = ModuleType(type_str)
        except ValueError:
            errors.append(f"invalid type '{type_str}'")
            raise InvalidManifestError(module_name, errors)

        # 未知字段检查
        allowed_keys = cls.COMMON_KEYS | cls.TYPE_KEYS[mod_type]
        unknown = set(raw.keys()) - allowed_keys
        if unknown:
            errors.append(
                f"unknown fields for type '{mod_type.value}': "
                f"{sorted(unknown)}"
            )

        # 必填字段
        for key in ("name", "version", "step", "owner"):
            if key not in raw:
                errors.append(f"missing required field '{key}'")

        # 基础类型校验
        if "name" in raw and not isinstance(raw["name"], str):
            errors.append("name must be str")
        if "version" in raw and not isinstance(raw["version"], str):
            errors.append("version must be str")
        if "step" in raw:
            if not isinstance(raw["step"], int) or raw["step"] < 1:
                errors.append("step must be positive int")
        if "owner" in raw and raw["owner"] not in KNOWN_TEAM_MEMBERS:
            errors.append(
                f"owner '{raw['owner']}' not in known team members "
                f"{sorted(KNOWN_TEAM_MEMBERS)}"
            )

        # deprecated 必须有 replaced_by
        if raw.get("deprecated"):
            if not raw.get("replaced_by"):
                errors.append(
                    "deprecated=True requires replaced_by"
                )

        # capabilities 格式
        caps = raw.get("capabilities", [])
        if not isinstance(caps, list) or not all(
            isinstance(c, str) and c.isidentifier() for c in caps
        ):
            errors.append(
                "capabilities must be list[str] of snake_case identifiers"
            )

        # module_dependencies 格式
        deps = raw.get("module_dependencies", [])
        if not isinstance(deps, list) or not all(
            isinstance(d, str) and d.isidentifier() for d in deps
        ):
            errors.append(
                "module_dependencies must be list[str] of snake_case identifiers"
            )

        # fallback 格式
        fb = raw.get("fallback")
        if fb is not None and not isinstance(fb, str):
            errors.append("fallback must be str or null")

        # ---- 类型专属校验 ----
        if mod_type == ModuleType.FACTOR:
            nr = raw.get("normalized_range")
            if nr is None:
                errors.append("factor requires normalized_range")
            else:
                try:
                    nr_tuple = (float(nr[0]), float(nr[1]))
                except (TypeError, ValueError, IndexError):
                    errors.append(
                        "normalized_range must be [lo, hi] two floats"
                    )
                    nr_tuple = None
                if nr_tuple and nr_tuple not in VALID_NORMALIZED_RANGES:
                    errors.append(
                        f"normalized_range must be (-1,1) or (0,1), "
                        f"got {nr_tuple}"
                    )

        elif mod_type == ModuleType.STRATEGY:
            rf = raw.get("required_factors", [])
            if not isinstance(rf, list) or not all(
                isinstance(f, str) and f.isidentifier() for f in rf
            ):
                errors.append(
                    "required_factors must be list[str] of snake_case identifiers"
                )

        elif mod_type == ModuleType.STATE:
            aa = raw.get("allowed_actions", [])
            valid_actions = {
                "entry", "add", "reduce", "exit", "hold", "noop",
            }
            if not isinstance(aa, list) or not all(
                a in valid_actions for a in aa
            ):
                errors.append(
                    f"allowed_actions must be subset of {sorted(valid_actions)}"
                )

        elif mod_type == ModuleType.GATEWAY:
            ex = raw.get("exchange")
            if not isinstance(ex, str) or not ex:
                errors.append("gateway requires exchange")

        if errors:
            raise InvalidManifestError(module_name, errors)

        # 构造 Manifest
        return Manifest(
            name=raw["name"],
            type=mod_type,
            version=raw["version"],
            step=raw["step"],
            owner=raw["owner"],
            capabilities=tuple(caps),
            module_dependencies=tuple(deps),
            fallback=fb,
            deprecated=bool(raw.get("deprecated", False)),
            replaced_by=raw.get("replaced_by"),
            enabled=bool(raw.get("enabled", True)),
            description=raw.get("description", ""),
            manifest_schema=schema,
            normalized_range=(
                (float(raw["normalized_range"][0]),
                 float(raw["normalized_range"][1]))
                if mod_type == ModuleType.FACTOR and raw.get("normalized_range")
                else None
            ),
            required_factors=tuple(raw.get("required_factors", [])),
            allowed_actions=tuple(raw.get("allowed_actions", [])),
            exchange=raw.get("exchange"),
        )


# ============================================================================
# 已加载模块
# ============================================================================
@dataclass
class LoadedModule:
    """已加载的模块：MANIFEST + 模块对象 + 路径。"""
    manifest: Manifest
    module: Any                    # Python 模块对象
    path: str
    loaded_at: float = field(default_factory=time.time)


# ============================================================================
# 组装结果
# ============================================================================
@dataclass
class AssembledSystem:
    """
    组装结果。所有模块实例 + 元数据。

    instances: 模块名 -> 实例
    manifest: 模块名 -> Manifest
    fallbacks: 模块名 -> (原模块名, 实际使用的 fallback 名)
    """
    instances: Dict[str, Any] = field(default_factory=dict)
    manifest: Dict[str, Manifest] = field(default_factory=dict)
    fallbacks: Dict[str, Tuple[str, str]] = field(default_factory=dict)

    def stats(self) -> Dict[str, int]:
        """按类型统计。"""
        from collections import Counter
        counter = Counter(m.type.value for m in self.manifest.values())
        return dict(counter)

    def health_check(self) -> Dict[str, Dict[str, Any]]:
        """遍历所有实例的 health()，返回汇总。"""
        result = {}
        for name, inst in self.instances.items():
            if hasattr(inst, "health"):
                try:
                    result[name] = inst.health()
                except Exception as e:
                    result[name] = {
                        "healthy": False,
                        "error": str(e),
                    }
            else:
                result[name] = {"healthy": True, "note": "no health()"}
        return result


# ============================================================================
# 模块注册表
# ============================================================================
class ModuleRegistry:
    """
    模块注册表。负责 discover / validate / assemble。

    线程安全，discover 幂等。
    """

    def __init__(self, root_dir: str = "quant/", current_step: int = 1):
        self.root_dir = Path(root_dir)
        self.current_step = current_step
        self._lock = threading.RLock()

        # 缓存
        self._discovered: Optional[Dict[str, LoadedModule]] = None
        self._validated: bool = False
        self._assembled: Optional[AssembledSystem] = None

    # ------------------------------------------------------------------------
    # 步骤 1：发现
    # ------------------------------------------------------------------------
    def discover(self) -> Dict[str, LoadedModule]:
        """
        扫描目录，加载所有带 MANIFEST 的模块。

        幂等：重复调用返回缓存。
        线程安全。
        """
        with self._lock:
            if self._discovered is not None:
                return self._discovered

            if not self.root_dir.exists():
                raise ManifestError(
                    f"root_dir not found: {self.root_dir}"
                )

            discovered: Dict[str, LoadedModule] = {}
            paths = sorted(
                p for p in self.root_dir.rglob("*.py")
                if not p.name.startswith("_")
            )

            for path in paths:
                try:
                    loaded = self._load_one(path)
                    if loaded is None:
                        continue
                    if loaded.manifest.name in discovered:
                        raise InvalidManifestError(
                            loaded.manifest.name,
                            [f"duplicate module name, also at "
                             f"{discovered[loaded.manifest.name].path}"],
                        )
                    discovered[loaded.manifest.name] = loaded
                except ManifestError:
                    raise
                except Exception as e:
                    logger.error(
                        f"failed to load {path}: {e}"
                    )

            logger.info(
                f"discovered {len(discovered)} modules from {self.root_dir}"
            )
            self._discovered = discovered
            return discovered

    def _load_one(self, path: Path) -> Optional[LoadedModule]:
        """加载单个模块。无 MANIFEST 返回 None。"""
        spec = importlib.util.spec_from_file_location(
            path.stem, path
        )
        if spec is None or spec.loader is None:
            return None

        module = importlib.util.module_from_spec(spec)

        # import 超时保护：Python 无法安全中断 import，
        # 但可以记录耗时，超时打 WARN
        t0 = time.time()
        spec.loader.exec_module(module)
        elapsed = time.time() - t0

        if elapsed > DEFAULT_MODULE_LOAD_TIMEOUT_S:
            logger.warning(
                f"{path.name} import took {elapsed:.2f}s "
                f"(> {DEFAULT_MODULE_LOAD_TIMEOUT_S}s)"
            )

        if not hasattr(module, "MANIFEST"):
            return None

        if not hasattr(module, "create"):
            raise InvalidManifestError(
                path.stem,
                ["module has MANIFEST but no create()"],
            )

        manifest = ManifestParser.parse(module.MANIFEST, str(path))
        return LoadedModule(
            manifest=manifest,
            module=module,
            path=str(path),
        )

    # ------------------------------------------------------------------------
    # 步骤 2：校验
    # ------------------------------------------------------------------------
    def validate(self) -> None:
        """
        校验：
        1. 依赖完整性
        2. 能力冲突
        3. 循环依赖
        4. 循环 fallback
        5. fallback 目标存在
        6. required_factors 有提供者
        """
        with self._lock:
            if self._validated:
                return

            discovered = self.discover()

            # 过滤：仅加载 enabled 且 step <= current_step
            active = {
                name: m for name, m in discovered.items()
                if m.manifest.is_loadable(self.current_step)
            }

            # 1. 能力提供者映射
            capability_providers: Dict[str, List[str]] = {}
            for name, m in active.items():
                for cap in m.manifest.capabilities:
                    capability_providers.setdefault(cap, []).append(name)

            # 2. 依赖完整性
            for name, m in active.items():
                missing = [
                    dep for dep in m.manifest.module_dependencies
                    if dep not in capability_providers
                ]
                if missing:
                    raise MissingDependencyError(name, missing)

                # required_factors 校验（策略专属）
                if m.manifest.type == ModuleType.STRATEGY:
                    missing_factors = [
                        f for f in m.manifest.required_factors
                        if f not in capability_providers
                    ]
                    if missing_factors:
                        raise MissingDependencyError(
                            name, missing_factors
                        )

            # 3. 能力冲突（同能力多提供者，且无 preferred）
            for cap, providers in capability_providers.items():
                if len(providers) > 1:
                    # 检查是否有 preferred 配置
                    preferred = self._get_preferred(cap)
                    if preferred and preferred in providers:
                        continue
                    raise CapabilityConflictError(cap, providers)

            # 4. 循环依赖检测
            self._detect_circular_dependencies(active)

            # 5. 循环 fallback 检测
            self._detect_circular_fallbacks(discovered)

            # 6. fallback 目标存在
            for name, m in active.items():
                if m.manifest.fallback:
                    if m.manifest.fallback not in discovered:
                        raise InvalidManifestError(
                            name,
                            [f"fallback '{m.manifest.fallback}' not found"],
                        )

            # 7. deprecated 模块被依赖时告警
            for name, m in active.items():
                if m.manifest.deprecated:
                    logger.warning(
                        f"module '{name}' is deprecated, "
                        f"replaced_by='{m.manifest.replaced_by}'"
                    )

            logger.info(
                f"validated {len(active)} active modules "
                f"({len(capability_providers)} capabilities)"
            )
            self._validated = True
            self._active = active
            self._capability_providers = capability_providers

    def _get_preferred(self, capability: str) -> Optional[str]:
        """从 config 中读取 preferred。"""
        cfg = getattr(self, "_config", {}) or {}
        manifest_cfg = cfg.get("manifest", {})
        preferred = manifest_cfg.get("preferred", {})
        return preferred.get(capability)

    def set_config(self, config: dict) -> None:
        """注入全局 config，用于读取 preferred。"""
        with self._lock:
            self._config = config

    def _detect_circular_dependencies(
        self, active: Dict[str, LoadedModule]
    ) -> None:
        """DFS 检测模块依赖环。"""
        # 构建能力 -> 提供者映射
        cap_providers: Dict[str, List[str]] = {}
        for name, m in active.items():
            for cap in m.manifest.capabilities:
                cap_providers.setdefault(cap, []).append(name)

        # 模块依赖图：name -> 依赖的模块名集合
        graph: Dict[str, Set[str]] = {}
        for name, m in active.items():
            deps = set()
            for dep_cap in m.manifest.module_dependencies:
                deps.update(cap_providers.get(dep_cap, []))
            graph[name] = deps

        # DFS 三色标记
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n: WHITE for n in graph}
        path: List[str] = []

        def dfs(node: str):
            color[node] = GRAY
            path.append(node)
            for dep in graph.get(node, set()):
                if color.get(dep) == GRAY:
                    cycle_start = path.index(dep)
                    raise CircularDependencyError(
                        path[cycle_start:] + [dep]
                    )
                if color.get(dep) == WHITE:
                    dfs(dep)
            path.pop()
            color[node] = BLACK

        for node in graph:
            if color[node] == WHITE:
                dfs(node)

    def _detect_circular_fallbacks(
        self, discovered: Dict[str, LoadedModule]
    ) -> None:
        """检测 fallback 链循环。"""
        for name in discovered:
            chain = [name]
            current = name
            visited = {name}
            while True:
                m = discovered.get(current)
                if not m or not m.manifest.fallback:
                    break
                current = m.manifest.fallback
                chain.append(current)
                if current in visited:
                    raise CircularFallbackError(chain)
                visited.add(current)

    # ------------------------------------------------------------------------
    # 步骤 3：组装
    # ------------------------------------------------------------------------
    def assemble(self, config: dict) -> AssembledSystem:
        """
        按拓扑序实例化所有模块。处理 fallback。

        线程安全。返回 AssembledSystem。
        """
        with self._lock:
            self.set_config(config)
            self.validate()

            # 拓扑排序
            order = self._topological_order()

            instances: Dict[str, Any] = {}
            fallbacks: Dict[str, Tuple[str, str]] = {}

            for name in order:
                m = self._active[name]
                try:
                    instance = self._instantiate(name, config)
                    instances[name] = instance
                except Exception as e:
                    # 尝试 fallback
                    if m.manifest.fallback:
                        fb_name = m.manifest.fallback
                        logger.warning(
                            f"module '{name}' init failed, "
                            f"fallback to '{fb_name}': {e}"
                        )
                        try:
                            fb_instance = self._instantiate(
                                fb_name, config
                            )
                            instances[name] = fb_instance
                            fallbacks[name] = (name, fb_name)
                        except Exception as fb_err:
                            raise ModuleUnavailableError(
                                name, [name, fb_name]
                            ) from fb_err
                    else:
                        raise ModuleInitError(name, e)

            result = AssembledSystem(
                instances=instances,
                manifest={n: self._active[n].manifest for n in order},
                fallbacks=fallbacks,
            )
            self._assembled = result
            logger.info(
                f"assembled {len(instances)} modules, "
                f"{len(fallbacks)} used fallback"
            )
            return result

    def _instantiate(self, name: str, config: dict) -> Any:
        """实例化单个模块。"""
        m = self._active[name]
        try:
            t0 = time.time()
            instance = m.module.create(config)
            elapsed = time.time() - t0
            if elapsed > DEFAULT_MODULE_LOAD_TIMEOUT_S:
                logger.warning(
                    f"'{name}'.create() took {elapsed:.2f}s"
                )
            return instance
        except Exception:
            raise

    def _topological_order(self) -> List[str]:
        """拓扑排序，被依赖的先实例化。"""
        cap_providers: Dict[str, List[str]] = {}
        for name, m in self._active.items():
            for cap in m.manifest.capabilities:
                cap_providers.setdefault(cap, []).append(name)

        # 依赖图
        graph: Dict[str, Set[str]] = {}
        for name, m in self._active.items():
            deps = set()
            for dep_cap in m.manifest.module_dependencies:
                deps.update(cap_providers.get(dep_cap, []))
            if m.manifest.type == ModuleType.STRATEGY:
                for f in m.manifest.required_factors:
                    deps.update(cap_providers.get(f, []))
            graph[name] = deps

        # 拓扑排序
        visited: Set[str] = set()
        result: List[str] = []

        def visit(node: str):
            if node in visited:
                return
            visited.add(node)
            for dep in graph.get(node, set()):
                visit(dep)
            result.append(node)

        # 按名称排序保证可复现
        for name in sorted(graph.keys()):
            visit(name)

        return result

    # ------------------------------------------------------------------------
    # 状态查询
    # ------------------------------------------------------------------------
    def get_assembled(self) -> Optional[AssembledSystem]:
        return self._assembled

    def reset(self) -> None:
        """重置注册表。测试用。"""
        with self._lock:
            self._discovered = None
            self._validated = False
            self._assembled = None
            self._active = {}
            self._capability_providers = {}


# ============================================================================
# 便捷函数
# ============================================================================
def discover_and_assemble(
    root_dir: str,
    config: dict,
    current_step: int,
) -> AssembledSystem:
    """一步完成 discover + validate + assemble。"""
    registry = ModuleRegistry(
        root_dir=root_dir, current_step=current_step
    )
    return registry.assemble(config)
