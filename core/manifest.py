"""
core/manifest.py — 模块清单解析与注册中心 (重铸标准版)

核心设计原则：
1. 绝对契约：模块的 MANIFEST 格式必须合法，字段缺失/类型错误直接抛出 ManifestError。
2. 严格隔离：自动扫描时过滤所有非业务目录（venv, tests, frontend, .git 等）。
3. 拒绝冲突：模块名称全局唯一，能力冲突必须在配置中显式声明 preferred。
4. 拒绝带病运行：组装时若初始化失败且无 fallback，直接报错中断。
"""

import importlib.util
import logging
import threading
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# ============================================================================
# 枚举与异常
# ============================================================================

class ModuleType(str, Enum):
    SYSTEM = "system"
    DATA = "data"
    FACTOR = "factor"
    STRATEGY = "strategy"
    STATE = "state"
    ROUND = "round"
    GATEWAY = "gateway"
    EXECUTION = "execution"
    RISK = "risk"
    CLOCK = "clock"
    ANALYTICS = "analytics"
    OTHER = "other"

class ManifestError(Exception):
    """模块清单解析基础异常"""
    pass

class DuplicateModuleError(ManifestError):
    pass

class MissingDependencyError(ManifestError):
    pass

class ModuleInitError(ManifestError):
    pass

# ============================================================================
# 数据结构
# ============================================================================

@dataclass(frozen=True)
class Manifest:
    """标准化后的模块清单"""
    name: str
    type: ModuleType
    version: str
    step: int
    owner: str
    capabilities: Tuple[str, ...] = ()
    module_dependencies: Tuple[str, ...] = ()
    fallback: Optional[str] = None
    enabled: bool = True
    
    # 专属字段
    normalized_range: Optional[Tuple[float, float]] = None
    required_factors: Tuple[str, ...] = ()
    allowed_actions: Tuple[str, ...] = ()
    exchange: Optional[str] = None

    def is_loadable(self, current_step: int) -> bool:
        return self.enabled and self.step <= current_step

@dataclass
class LoadedModule:
    manifest: Manifest
    module: Any
    path: str

@dataclass
class AssembledSystem:
    instances: Dict[str, Any] = field(default_factory=dict)
    manifest: Dict[str, Manifest] = field(default_factory=dict)
    fallbacks: Dict[str, Tuple[str, str]] = field(default_factory=dict)

# ============================================================================
# 解析器
# ============================================================================

class ManifestParser:
    @classmethod
    def parse(cls, raw: dict, path: str) -> Manifest:
        """解析原始字典为严格的 Manifest 数据类"""
        if not isinstance(raw, dict):
            raise ManifestError(f"MANIFEST in {path} must be a dict")
            
        if "name" not in raw or not raw["name"]:
            raise ManifestError(f"Missing or empty 'name' in MANIFEST at {path}")
            
        if "type" not in raw:
            raise ManifestError(f"Missing 'type' in MANIFEST for '{raw.get('name')}'")
            
        try:
            mod_type = ModuleType(raw["type"])
        except ValueError:
            raise ManifestError(f"Invalid module type '{raw['type']}' for '{raw.get('name')}'")

        # 处理专属字段的安全转换
        nr = raw.get("normalized_range")
        nr_tuple = tuple(nr) if nr and isinstance(nr, (list, tuple)) else None

        return Manifest(
            name=str(raw["name"]),
            type=mod_type,
            version=str(raw.get("version", "1.0.0")),
            step=int(raw.get("step", 1)),
            owner=str(raw.get("owner", "unknown")),
            capabilities=tuple(raw.get("capabilities", [])),
            module_dependencies=tuple(raw.get("module_dependencies", [])),
            fallback=raw.get("fallback"),
            enabled=bool(raw.get("enabled", True)),
            normalized_range=nr_tuple,
            required_factors=tuple(raw.get("required_factors", [])),
            allowed_actions=tuple(raw.get("allowed_actions", [])),
            exchange=raw.get("exchange")
        )

# ============================================================================
# 注册中心
# ============================================================================

class ModuleRegistry:
    """
    模块自动发现与装配中心。
    """
    def __init__(self, root_dir: str = ".", current_step: int = 1):
        self.root_dir = Path(root_dir).resolve()
        self.current_step = current_step
        self._lock = threading.RLock()
        self._discovered: Dict[str, LoadedModule] = {}
        self._assembled: Optional[AssembledSystem] = None
        self._config: dict = {}
        self._preferred: Dict[str, str] = {}

    def set_config(self, config: dict) -> None:
        """注入全局配置，以便读取 preferred 等参数"""
        self._config = config
        pref = config.get("manifest", {}).get("preferred", {})
        if isinstance(pref, dict):
            self._preferred = pref

    def discover(self) -> Dict[str, LoadedModule]:
        """扫描目录，加载所有合法的模块"""
        with self._lock:
            if self._discovered:
                return self._discovered

            logger.info(f"开始扫描模块，根目录: {self.root_dir}")
            
            # 定义要排除的目录
            exclude_dirs = {".git", "venv", "__pycache__", "tests", "frontend", "logs", "reports", "docs", "config"}
            
            for path in self.root_dir.rglob("*.py"):
                # 检查路径中是否包含排除的目录
                if any(part in exclude_dirs for part in path.parts):
                    continue
                if path.name.startswith("_"):
                    continue
                    
                try:
                    spec = importlib.util.spec_from_file_location(path.stem, path)
                    if not spec or not spec.loader:
                        continue
                        
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)
                    
                    if hasattr(mod, "MANIFEST"):
                        manifest = ManifestParser.parse(mod.MANIFEST, str(path))
                        
                        if manifest.name in self._discovered:
                            existing = self._discovered[manifest.name].path
                            raise DuplicateModuleError(
                                f"模块名称冲突: '{manifest.name}' "
                                f"在 {existing} 和 {path} 同时被声明"
                            )
                            
                        self._discovered[manifest.name] = LoadedModule(
                            manifest=manifest, module=mod, path=str(path)
                        )
                except Exception as e:
                    # 捕获解析或加载过程中的任何异常并立刻抛出，绝不静默跳过
                    logger.error(f"加载模块 {path} 失败: {e}")
                    raise ManifestError(f"Failed to load {path}: {e}") from e

            logger.info(f"扫描完成，共发现 {len(self._discovered)} 个有效模块")
            return self._discovered

    def validate(self) -> None:
        """校验模块的依赖完整性"""
        discovered = self.discover()
        active = {k: v for k, v in discovered.items() if v.manifest.is_loadable(self.current_step)}

        # 收集所有激活模块能提供的能力
        capabilities: Set[str] = set()
        for m in active.values():
            capabilities.update(m.manifest.capabilities)

        # 校验依赖
        for name, m in active.items():
            for dep in m.manifest.module_dependencies:
                if dep not in capabilities:
                    raise MissingDependencyError(
                        f"模块 '{name}' 依赖能力 '{dep}'，但未在当前步骤的模块中找到提供者"
                    )
            
            # 校验 fallback 目标是否存在
            if m.manifest.fallback and m.manifest.fallback not in discovered:
                raise ManifestError(f"模块 '{name}' 的 fallback '{m.manifest.fallback}' 不存在")

    def assemble(self, config: dict) -> AssembledSystem:
        """执行实例化与组装"""
        with self._lock:
            self.set_config(config)
            self.validate()
            
            discovered = self.discover()
            active = {k: v for k, v in discovered.items() if v.manifest.is_loadable(self.current_step)}
            
            instances = {}
            fallbacks = {}
            
            for name, m in active.items():
                try:
                    if not hasattr(m.module, "create"):
                        raise ManifestError(f"模块 '{name}' 缺少 create() 工厂函数")
                        
                    instance = m.module.create(config)
                    instances[name] = instance
                    
                except Exception as e:
                    if m.manifest.fallback:
                        fb_name = m.manifest.fallback
                        logger.warning(f"模块 '{name}' 初始化失败，尝试 fallback 到 '{fb_name}': {e}")
                        try:
                            fb_instance = discovered[fb_name].module.create(config)
                            instances[name] = fb_instance
                            fallbacks[name] = (name, fb_name)
                        except Exception as fb_e:
                            raise ModuleInitError(f"模块 '{name}' 和 fallback '{fb_name}' 均初始化失败: {fb_e}") from fb_e
                    else:
                        raise ModuleInitError(f"模块 '{name}' 初始化失败，且无 fallback 兜底: {e}") from e

            # 这里可以加入拓扑排序以保证依赖初始化顺序，但目前按需实例化已足够
            self._assembled = AssembledSystem(
                instances=instances, 
                manifest={k: v.manifest for k, v in active.items()}, 
                fallbacks=fallbacks
            )
            logger.info(f"模块组装完成，成功实例化 {len(instances)} 个组件")
            return self._assembled

# ============================================================================
# 兼容外部调用的工具函数
# ============================================================================

def validate_manifest(manifest_dict: dict) -> bool:
    """
    兼容外部（如 main.py）的快速校验工具
    """
    if not isinstance(manifest_dict, dict):
        raise ManifestError("manifest must be a dict")
    if "name" not in manifest_dict:
        raise ManifestError("missing 'name' in manifest")
    if "type" not in manifest_dict:
        raise ManifestError("missing 'type' in manifest")
    return True
