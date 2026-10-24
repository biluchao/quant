# DEPLOY_STEP1.md — Step 1 部署手册

> 版本：v1.0.0
> 生效日期：Step 1 启动日
> 维护人：张三（系统部署）、乐高（执行配置）、比蒙（策略验证）
> 适用对象：三个月后的自己，或接手部署的新同事
> 关联文档：CONVENTION.md、FACTOR_SPEC.md、PARAM_TABLE.md

---

## 0. 总则

**0.1 部署手册不是写给“会的人”看的，是写给“三个月后的自己”看的。**
三个月后你忘了所有命令，只能靠这份手册重新把系统跑起来。**每个命令必须可复制粘贴，每个步骤必须有验证方法。**

**0.2 三阶段部署，逐层递进。**
```
回测（30天历史） → 影子模式（72小时实时） → 小资金实盘（1000U，1周）
```
每一阶段有明确的验收标准，通过才进入下一阶段。**不跳级。**

**0.3 部署不是“跑起来”就完了，是“跑对了”才算完。**
跑起来只是第一步，跑对（信号正确、订单正确、风控正确、归因正确）才算部署完成。

**0.4 任何时候出问题，先停，再查，不要边跑边修。**
实盘系统边跑边修 = 边开车边修发动机。**发现问题 → 紧急停止 → 排查 → 修复 → 重新部署。**

---

## 1. 环境要求

### 1.1 硬件要求

| 项目 | 最低配置 | 推荐配置 | 说明 |
|---|---|---|---|
| CPU | 2 核 | 4 核 | 因子计算、状态机需要算力 |
| 内存 | 4 GB | 8 GB | 内存里保存 K 线缓冲、轮次状态 |
| 磁盘 | 50 GB SSD | 100 GB SSD | 历史数据 + 日志 |
| 网络 | 10 Mbps | 50 Mbps | WebSocket 数据流 + REST 请求 |
| 时区 | UTC | UTC | **必须是 UTC**，避免时间混乱 |

**部署位置选择：**

| 位置 | REST 延迟 | 说明 |
|---|---|---|
| 上海直连 | 30-50ms | 可用，但延迟偏高 |
| 东京 AWS | 5-10ms | **推荐**，离币安 API 服务器近 |
| 新加坡 AWS | 10-20ms | 可用 |
| 香港 | 15-25ms | 可用 |

### 1.2 软件要求

| 软件 | 版本 | 安装方式 |
|---|---|---|
| 操作系统 | Ubuntu 22.04 LTS | 推荐 |
| Python | 3.11+ | `apt install python3.11` |
| pip | 最新 | `python3.11 -m ensurepip` |
| Git | 2.x | `apt install git` |
| SQLite | 3.35+ | 系统自带 |
| NTP | chrony | `apt install chrony` |

### 1.3 Python 依赖

`requirements.txt`（锁版本）：

```
numpy==1.26.4
pandas==2.2.1
requests==2.31.0
websockets==12.0
pyyaml==6.0.1
fastapi==0.109.0
uvicorn==0.27.0
scipy==1.12.0
python-dotenv==1.0.0
pytest==8.0.0
pytest-cov==4.1.0
```

安装命令：
```bash
cd /opt/quant
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**验证：**
```bash
python -c "import numpy, pandas, requests, websockets, yaml, fastapi, scipy; print('OK')"
```

---

## 2. 目录结构与权限

### 2.1 目录结构

```
/opt/quant/                          # 主目录
├── quant/                           # 代码
├── config/                          # 配置
│   ├── base.yaml
│   ├── step1.yaml
│   ├── symbols/
│   │   └── btcusdt.yaml
│   └── snapshots/                   # 参数快照
├── data/                            # 数据
│   ├── live_quant.db                # 实盘数据库
│   ├── shadow_quant.db              # 影子数据库
│   ├── backtest_quant.db            # 回测数据库
│   └── klines/                      # 历史 K 线
├── logs/                            # 日志
│   ├── live/
│   ├── shadow/
│   └── backtest/
├── reports/                         # 报告
│   ├── backtest/
│   ├── shadow/
│   └── live/
├── venv/                            # 虚拟环境
├── .env                             # 密钥（不提交 Git）
├── .env.example                     # 密钥模板
└── requirements.txt
```

### 2.2 创建目录并设置权限

```bash
# 创建目录
sudo mkdir -p /opt/quant/{data,logs/{live,shadow,backtest},reports/{backtest,shadow,live},config/snapshots}
sudo chown -R $USER:$USER /opt/quant

# 设置权限
chmod 700 /opt/quant/data
chmod 700 /opt/quant/logs
chmod 600 /opt/quant/.env
```

**验证：**
```bash
ls -la /opt/quant/
# 确认 data/ 和 logs/ 权限是 700，.env 权限是 600
```

---

## 3. 配置准备

### 3.1 获取 API Key

**币安合约 API Key 申请步骤：**

1. 登录币安 → 用户中心 → API 管理
2. 创建 API → 选择“系统生成”
3. 填写名称：`quant-live`
4. 完成安全验证
5. **权限设置（关键）：**
   - ✓ 启用合约
   - ✓ 启用读取
   - ✗ **不要勾选“启用提现”**
   - ✗ **不要勾选“启用现货”**（Step 1 只用合约）
6. **IP 白名单（关键）：**
   - 添加服务器公网 IP
   - 不配白名单 = Key 泄露后任何人可用

**验证 Key 权限：**
```bash
curl -X GET "https://fapi.binance.com/fapi/v2/account" \
  -H "X-MBX-APIKEY: YOUR_API_KEY" \
  -d "timestamp=$(date +%s000)" \
  -d "signature=..."
# 返回账户信息 = 权限正常
```

### 3.2 配置 .env

`.env.example`：
```bash
# 币安 API
BINANCE_API_KEY=your_api_key_here
BINANCE_API_SECRET=your_api_secret_here
BINANCE_REST_BASE_URL=https://fapi.binance.com
BINANCE_WS_BASE_URL=wss://fstream.binance.com

# 系统
QUANT_ROOT=/opt/quant
LOG_DIR=/opt/quant/logs
DB_PATH=/opt/quant/data/live_quant.db
```

**操作：**
```bash
cp /opt/quant/.env.example /opt/quant/.env
chmod 600 /opt/quant/.env
# 编辑 .env，填入真实 API Key/Secret
vim /opt/quant/.env
```

**验证：**
```bash
cd /opt/quant
python -c "from dotenv import load_dotenv; import os; load_dotenv(); print('API Key:', os.getenv('BINANCE_API_KEY')[:8] + '...')"
```

### 3.3 配置文件校验

```bash
cd /opt/quant
python scripts/validate_config.py --config config --symbol btcusdt
```

**输出应该是：**
```
[OK] base.yaml 加载成功
[OK] step1.yaml 加载成功
[OK] btcusdt.yaml 加载成功
[OK] 参数类型校验通过
[OK] 参数范围校验通过
[OK] 参数组约束通过
[OK] 安全上限校验通过
```

如果报错，根据提示修改配置。

---

## 4. 系统环境准备

### 4.1 时区和 NTP

```bash
# 设置时区为 UTC
sudo timedatectl set-timezone UTC
timedatectl
# 确认：Time zone: UTC (UTC, +0000)

# 安装并配置 chrony
sudo apt install -y chrony
sudo systemctl enable chrony
sudo systemctl start chrony
chronyc tracking
# 确认：System time offset < 10ms
```

**验证：**
```bash
date -u
# 输出的时间应与 UTC 一致
```

### 4.2 网络延迟测试

```bash
# 测试到币安的 REST 延迟
ping -c 5 fapi.binance.com
# 期望：avg < 50ms（上海直连）/ < 10ms（东京 AWS）

# 测试 WebSocket 延迟
python -c "
import time, websockets, asyncio
async def test():
    t0 = time.time()
    async with websockets.connect('wss://fstream.binance.com/ws/btcusdt@kline_3m') as ws:
        await ws.recv()
        print(f'WS RTT: {(time.time()-t0)*1000:.1f}ms')
asyncio.run(test())
"
```

**如果延迟 > 100ms，考虑更换部署位置。**

### 4.3 交易所维护窗口检查

```bash
# 查询交易所状态
curl -s https://fapi.binance.com/fapi/v1/ping
# 返回空 = 正常

# 查询交易所时间
curl -s https://fapi.binance.com/fapi/v1/time
# 返回 serverTime，与本地时间差应 < 500ms
```

---

## 5. 数据准备

### 5.1 历史 K 线导入

**方式 1：从币安 REST 拉取**

```bash
cd /opt/quant
python scripts/download_klines.py \
  --symbol btcusdt \
  --interval 3m \
  --start 2024-03-01 \
  --end 2024-06-01 \
  --db /opt/quant/data/backtest_quant.db
```

**方式 2：从备份导入**

```bash
cd /opt/quant
sqlite3 /opt/quant/data/backtest_quant.db < /opt/quant/data/klines/btcusdt_3m_backup.sql
```

**验证：**
```bash
sqlite3 /opt/quant/data/backtest_quant.db "
SELECT COUNT(*) as total,
       MIN(ts) as first_ts,
       MAX(ts) as last_ts
FROM klines
WHERE symbol='btcusdt' AND interval='3m';
"
# 期望：total > 40000（90天 * 480根/天）
```

### 5.2 盘口快照导入

```bash
cd /opt/quant
python scripts/download_snapshots.py \
  --symbol btcusdt \
  --start 2024-03-01 \
  --end 2024-06-01 \
  --db /opt/quant/data/backtest_quant.db
```

**验证：**
```bash
sqlite3 /opt/quant/data/backtest_quant.db "
SELECT COUNT(*) FROM snapshots WHERE symbol='btcusdt';
"
```

**注意：** 如果没有盘口快照，回测会使用合成盘口。合成盘口的 OBI 因子值仅供参考，报告中会标记 `synthetic_snapshots` 数量。

### 5.3 预热数据验证

```bash
cd /opt/quant
python scripts/verify_warmup.py \
  --db /opt/quant/data/backtest_quant.db \
  --symbol btcusdt \
  --warmup-bars 500
```

**期望：**
```
[OK] K线总数: 43200
[OK] 预热窗口: 500 根
[OK] 分位数窗口填充: 500/500
[OK] 首根可决策K线时间戳: 2024-03-03 08:00:00
```

---

## 6. 部署流程（10 步）

### 6.1 完整部署步骤

```
步骤 1: 环境准备（第 1-4 节）
步骤 2: 代码部署
步骤 3: 配置准备
步骤 4: 数据准备
步骤 5: 单元测试
步骤 6: 回测验证
步骤 7: 影子模式部署
步骤 8: 影子模式运行（72 小时）
步骤 9: 实盘部署
步骤 10: 实盘运行（1000U，1周）
```

### 6.2 步骤 2：代码部署

```bash
# 克隆代码
cd /opt
git clone git@github.com:your-repo/quant.git
cd /opt/quant

# 切换分支
git checkout step1-minimal

# 确认版本
git log -1 --oneline
# 期望：看到最新的 commit

# 创建虚拟环境
python3.11 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 验证
python -c "import quant; print('quant imported OK')"
```

### 6.3 步骤 5：单元测试

```bash
cd /opt/quant
source venv/bin/activate

# 跑单元测试
pytest tests/unit -v --cov=quant --cov-report=term-missing

# 期望：全部通过，覆盖率 >= 40%
```

**如果测试失败，不允许进入下一步。**

### 6.4 步骤 6：回测验证

```bash
cd /opt/quant
source venv/bin/activate

# 跑回测
python run_backtest.py \
  --config config \
  --symbol btcusdt \
  --start 2024-03-01 \
  --end 2024-06-01 \
  --seed 42 \
  --initial_equity 10000 \
  --backtest_db /opt/quant/data/backtest_quant.db \
  --output_dir /opt/quant/reports/backtest
```

**验收标准（必须全部满足）：**

| 指标 | 通过标准 | 当前值 |
|---|---|---|
| 夏普比率 | > 1.0 | |
| 最大回撤 | < 8% | |
| 假信号率 | < 15% | |
| 胜率 | > 40% | |
| 盈亏比 | > 1.5 | |
| 轮次数量 | > 100 | |
| 每笔可回放 | 是 | |

**如果未通过，调整参数，重新回测，直到通过。**

### 6.5 步骤 7：影子模式部署

```bash
cd /opt/quant
source venv/bin/activate

# 前台启动（首次部署用前台，观察日志）
python run_shadow.py \
  --config config \
  --symbol btcusdt \
  --min-hours 72 \
  --initial_equity 10000
```

**确认启动成功：**
```
[INFO] 影子模式启动 symbol=btcusdt min_hours=72 pid=12345
[INFO] 时钟偏移 market_offset=12.3 ms
[INFO] 历史 K 线预热 500 根
[INFO] 数据流就绪 3 个
[INFO] 状态机/轮次/因子 就绪
[INFO] 信号追踪器就绪 horizon=3/6/12/24 bars
[INFO] 影子模式启动，进入主循环
```

**如果启动失败，根据错误信息排查（见第 10 节故障排查）。**

### 6.6 步骤 8：影子模式运行（72 小时）

**后台运行方式（72 小时不间断）：**

```bash
# 用 systemd 管理（推荐）
sudo cp /opt/quant/deploy/systemd/quant-shadow.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable quant-shadow
sudo systemctl start quant-shadow

# 查看状态
sudo systemctl status quant-shadow

# 查看日志
journalctl -u quant-shadow -f
```

**或者用 nohup：**
```bash
cd /opt/quant
source venv/bin/activate
nohup python run_shadow.py \
  --config config \
  --symbol btcusdt \
  --min-hours 72 \
  --initial_equity 10000 \
  > /opt/quant/logs/shadow/nohup.log 2>&1 &
echo $! > /opt/quant/logs/shadow/shadow.pid
```

**影子模式验收标准（72 小时后检查）：**

| 检查项 | 通过标准 | 检查命令 |
|---|---|---|
| 无崩溃 | 运行 72 小时无异常退出 | `systemctl status quant-shadow` |
| 零幽灵订单 | 对账偏差 = 0 | 查看报告 `reports/shadow/` |
| K线OHLC偏差 | < 0.1% | 查看报告 `kline_consistency` |
| 信号数量 | 正常范围（参考回测频率） | 查看报告 `signal_stats` |
| 因子IC | 在正常范围 | 查看报告 `factor_ic` |
| 数据降级 | 无或短暂 | 查看日志 `degraded=False` |

**如果未通过，排查问题，修复后重新跑 72 小时。**

### 6.7 步骤 9：实盘部署

**前置检查（必须全部通过）：**

```bash
cd /opt/quant
python scripts/preflight_live.py --config config --symbol btcusdt
```

**期望输出（20 项全过）：**
```
[✓] config: API Key 长度正确
[✓] api_permissions: 期货权限正常
[✓] api_trade_permission: 可下单+撤单
[✓] server_time: offset=23ms
[✓] websocket: mark_price=67000.5
[✓] account_equity: equity=1000.00 USDT
[✓] no_open_orders: count=0
[✓] no_positions: active=0
[✓] server_side_stop: 止损单可挂
[✓] leverage: current=3 target=3
[✓] margin_ratio: ratio=0.0000
[✓] rate_limiter: loaded
[✓] symbol_filters: step=0.001 tick=0.1 minNotional=5.0
[✓] param_consistency: 一致
[✓] disk: free=95.2GB
[✓] state_db: WAL mode OK
[✓] emergency_channel: ready
[✓] clock_offset: offset=12.3ms
[✓] factor_warmup: bars=500/500
[✓] network_latency: avg=8.5ms
[✓] kill_switch: kill file absent
PREFLIGHT 21/21 通过
```

**如果任何一项失败，不允许启动实盘。**

### 6.8 步骤 10：实盘运行

**首次实盘启动：**

```bash
cd /opt/quant
source venv/bin/activate

# 前台启动（首次，观察 10 分钟）
python run_live.py \
  --config config \
  --symbol btcusdt \
  --confirm
```

**确认启动成功：**
```
[INFO] LIVE 启动 symbol=btcusdt pid=12345
[WARNING] 实盘模式，真实资金，任何异常立即人工介入
[INFO] 实盘权益来自交易所: 1000.00
[INFO] PREFLIGHT CHECK 开始
[INFO] PREFLIGHT 21/21 通过
[INFO] 保护期: 前 20 轮半仓
[INFO] 首单测试量: 0.001 btcusdt
[INFO] LIVE 启动完成
[INFO] 进入主循环
```

**观察 10 分钟，确认：**
- 日志无 ERROR
- 心跳正常（每 30 秒一条 `status` 日志）
- 如果有信号，确认订单发出、成交、止损挂上

**10 分钟后转后台：**
```bash
# 停掉前台进程（Ctrl+C，优雅停机）
# 用 systemd 启动
sudo cp /opt/quant/deploy/systemd/quant-live.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable quant-live
sudo systemctl start quant-live
sudo systemctl status quant-live
```

**实盘验收标准（1 周后检查）：**

| 检查项 | 通过标准 | 检查方式 |
|---|---|---|
| 成交率 | > 95% | 查看报告 `reports/live/` |
| 滑点偏差 | < 30%（vs 模型预估） | 对比 `slippage_model` 与真实成交 |
| API 错误 | 0 次致命错误 | 查看日志 `grep ERROR` |
| 对账偏差 | < 1% | 查看 `daily_reconciliation` |
| 紧急停止 | 未触发 | 查看报告 `emergency_triggered` |
| 保护期 | 已完成 20 轮 | 查看 `protection_mode_ended` |

---

## 7. systemd 服务配置

### 7.1 quant-live.service

```ini
[Unit]
Description=Quant Live Trading System
After=network.target chrony.service
Wants=chrony.service

[Service]
Type=simple
User=quant
Group=quant
WorkingDirectory=/opt/quant
Environment="PATH=/opt/quant/venv/bin:/usr/local/bin:/usr/bin:/bin"
EnvironmentFile=/opt/quant/.env
ExecStart=/opt/quant/venv/bin/python /opt/quant/run_live.py --config /opt/quant/config --symbol btcusdt --confirm
Restart=on-failure
RestartSec=10
StartLimitBurst=3
StartLimitIntervalSec=300
TimeoutStopSec=30
KillSignal=SIGTERM

# 日志
StandardOutput=journal
StandardError=journal
SyslogIdentifier=quant-live

# 安全
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/opt/quant/data /opt/quant/logs /opt/quant/reports /opt/quant/config/snapshots

[Install]
WantedBy=multi-user.target
```

**注意：**
- `Restart=on-failure`：只有异常退出才重启，正常退出（退出码 0）不重启
- `StartLimitBurst=3`：5 分钟内最多重启 3 次，超过则停止（防止崩溃循环）
- `TimeoutStopSec=30`：给优雅停机 30 秒

### 7.2 quant-shadow.service

```ini
[Unit]
Description=Quant Shadow Trading System
After=network.target chrony.service

[Service]
Type=simple
User=quant
Group=quant
WorkingDirectory=/opt/quant
Environment="PATH=/opt/quant/venv/bin:/usr/local/bin:/usr/bin:/bin"
EnvironmentFile=/opt/quant/.env
ExecStart=/opt/quant/venv/bin/python /opt/quant/run_shadow.py --config /opt/quant/config --symbol btcusdt --min-hours 72
Restart=on-failure
RestartSec=10
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

### 7.3 常用 systemd 命令

```bash
# 启动
sudo systemctl start quant-live

# 停止（优雅停机）
sudo systemctl stop quant-live

# 重启
sudo systemctl restart quant-live

# 查看状态
sudo systemctl status quant-live

# 查看日志
journalctl -u quant-live -f
journalctl -u quant-live --since "1 hour ago"

# 开机自启
sudo systemctl enable quant-live

# 禁用自启
sudo systemctl disable quant-live
```

---

## 8. 健康检查

### 8.1 一键健康检查脚本

`scripts/healthcheck.sh`：

```bash
#!/bin/bash
# 量化系统健康检查

echo "=== 量化系统健康检查 ==="
echo "时间: $(date -u)"

# 1. 进程检查
echo ""
echo "[1] 进程状态"
if pgrep -f "run_live.py" > /dev/null; then
    echo "  ✓ live 进程运行中 (PID: $(pgrep -f run_live.py))"
else
    echo "  ✗ live 进程未运行"
fi

# 2. 心跳检查
echo ""
echo "[2] 心跳检查"
if [ -f /tmp/quant_live_heartbeat ]; then
    last_hb=$(cat /tmp/quant_live_heartbeat)
    now=$(date +%s)
    diff=$((now - ${last_hb%.*}))
    if [ $diff -lt 120 ]; then
        echo "  ✓ 心跳正常 (${diff}s 前)"
    else
        echo "  ✗ 心跳超时 (${diff}s 前)"
    fi
else
    echo "  ✗ 心跳文件不存在"
fi

# 3. 时钟偏移
echo ""
echo "[3] 时钟偏移"
offset=$(chronyc tracking | grep "System time" | awk '{print $4}')
echo "  offset: ${offset}s"

# 4. 磁盘空间
echo ""
echo "[4] 磁盘空间"
free_gb=$(df -BG /opt/quant | tail -1 | awk '{print $4}' | tr -d 'G')
if [ $free_gb -gt 10 ]; then
    echo "  ✓ 磁盘剩余 ${free_gb}GB"
else
    echo "  ✗ 磁盘剩余 ${free_gb}GB < 10GB"
fi

# 5. 日志错误
echo ""
echo "[5] 最近 1 小时 ERROR 日志"
error_count=$(journalctl -u quant-live --since "1 hour ago" | grep -c "ERROR")
if [ $error_count -eq 0 ]; then
    echo "  ✓ 无 ERROR"
else
    echo "  ✗ $error_count 条 ERROR"
    journalctl -u quant-live --since "1 hour ago" | grep "ERROR" | tail -5
fi

# 6. 对账偏差
echo ""
echo "[6] 最新对账报告"
latest_report=$(ls -t /opt/quant/reports/live/live_report_*.json 2>/dev/null | head -1)
if [ -n "$latest_report" ]; then
    echo "  报告: $latest_report"
    python -c "
import json
with open('$latest_report') as f:
    r = json.load(f)
print(f\"  权益: {r['summary']['equity']:.2f} USDT\")
print(f\"  活跃轮次: {r['summary']['active_rounds']}\")
print(f\"  日盈亏: {r['summary']['daily_pnl']:+.2f} USDT\")
"
fi

# 7. Kill switch
echo ""
echo "[7] Kill switch"
if [ -f /tmp/quant_live_kill ]; then
    echo "  ✗ KILL SWITCH 文件存在！"
else
    echo "  ✓ Kill switch 未触发"
fi

echo ""
echo "=== 检查完成 ==="
```

**使用：**
```bash
chmod +x /opt/quant/scripts/healthcheck.sh
/opt/quant/scripts/healthcheck.sh
```

### 8.2 健康检查频率

| 场景 | 频率 | 检查内容 |
|---|---|---|
| 实盘保护期（前 1 周） | 每小时 | 完整检查 |
| 实盘稳定期 | 每 4 小时 | 进程+心跳+磁盘 |
| 影子模式 | 每天 | 完整检查 |
| 出问题时 | 立即 | 完整检查 + 日志分析 |

---

## 9. 日志位置和查看方式

### 9.1 日志路径

| 模式 | 路径 | 说明 |
|---|---|---|
| live | `/opt/quant/logs/live/` | 按小时滚动 |
| shadow | `/opt/quant/logs/shadow/` | 按小时滚动 |
| backtest | `/opt/quant/logs/backtest/` | 按运行生成 |
| systemd | `journalctl -u quant-live` | 进程级日志 |

### 9.2 常用查看命令

```bash
# 实时日志
tail -f /opt/quant/logs/live/quant_$(date +%Y%m%d_%H).log

# 查看最近的 ERROR
grep ERROR /opt/quant/logs/live/*.log | tail -20

# 查看最近的 CRITICAL
grep CRITICAL /opt/quant/logs/live/*.log | tail -10

# 查看今天的订单日志
grep order_submit /opt/quant/logs/live/quant_$(date +%Y%m%d)*.log

# 查看状态迁移
grep state_transition /opt/quant/logs/live/quant_$(date +%Y%m%d)*.log | tail -20

# 查看 systemd 日志
journalctl -u quant-live --since "1 hour ago"

# 查看 systemd 错误
journalctl -u quant-live --since "1 hour ago" -p err
```

### 9.3 日志清理

日志按小时滚动，保留 30 天。旧日志自动删除。如需手动清理：

```bash
# 删除 30 天前的日志
find /opt/quant/logs/live/ -name "*.log" -mtime +30 -delete

# 查看日志目录大小
du -sh /opt/quant/logs/live/
```

---

## 10. 故障排查

### 10.1 常见故障 Top 10

| # | 故障 | 症状 | 排查 | 修复 |
|---|---|---|---|---|
| 1 | 进程启动失败 | `systemctl status` 显示 failed | 看 `journalctl -u quant-live` | 根据错误信息修复 |
| 2 | 心跳超时 | 心跳文件 > 120s 未更新 | 检查进程是否假死 | 重启进程，查看日志 |
| 3 | 时钟偏移过大 | `chronyc tracking` offset > 500ms | NTP 服务不正常 | `systemctl restart chrony` |
| 4 | 磁盘满 | 日志写入失败 | `df -h` | 清理日志或扩容 |
| 5 | WebSocket 断线 | 日志频繁 `disconnect` | 网络问题或交易所维护 | 等待重连，或更换网络 |
| 6 | 订单被拒 | `ERROR order_rejected code=-2010` | 余额不足 | 充值或减小仓位 |
| 7 | 限频 | `ERROR rate_limit_exceeded` | 请求太频繁 | 降低请求频率 |
| 8 | 幽灵订单 | 对账偏差 > 0 | 订单状态不同步 | 冻结轮次，手动对账 |
| 9 | 保证金率过高 | `CRITICAL margin_ratio > 0.8` | 仓位过重 | 紧急减仓 |
| 10 | Kill switch 触发 | `/tmp/quant_live_kill` 存在 | 有人手动触发 | 确认原因后删除文件 |

### 10.2 紧急停止操作

**方式 1：Kill switch 文件（最快）**
```bash
# 触发紧急停止
touch /tmp/quant_live_kill
# 系统会在下一个主循环检测到，触发紧急停止
# 紧急停止 = 撤所有挂单 + 平所有仓位 + 退出进程
```

**方式 2：systemd 停止**
```bash
sudo systemctl stop quant-live
# 优雅停机：保仓 + 挂服务器端止损
```

**方式 3：API 紧急停止**
```bash
curl -X POST http://localhost:8000/api/emergency_stop \
  -H "Content-Type: application/json" \
  -d '{"token": "your_emergency_token", "reason": "manual"}'
```

**方式 4：直接去币安后台手动平仓**

### 10.3 部署回滚

```bash
# 1. 停止当前版本
sudo systemctl stop quant-live

# 2. 回滚代码
cd /opt/quant
git log --oneline -10  # 找到要回滚到的 commit
git checkout <commit-hash>

# 3. 回滚参数（如果有快照）
python scripts/rollback_params.py --to 2024-06-15_120000

# 4. 重新安装依赖（如果依赖有变化）
pip install -r requirements.txt

# 5. 重新启动
sudo systemctl start quant-live

# 6. 验证
/opt/quant/scripts/healthcheck.sh
```

### 10.4 数据备份与恢复

**备份：**
```bash
# 备份数据库
sqlite3 /opt/quant/data/live_quant.db ".backup /opt/quant/data/backup/live_quant_$(date +%Y%m%d).db"

# 备份配置
tar -czf /opt/quant/data/backup/config_$(date +%Y%m%d).tar.gz /opt/quant/config/

# 自动备份（crontab）
# 每天凌晨 2 点备份
0 2 * * * sqlite3 /opt/quant/data/live_quant.db ".backup /opt/quant/data/backup/live_quant_$(date +\%Y\%m\%d).db"
```

**恢复：**
```bash
# 停止系统
sudo systemctl stop quant-live

# 恢复数据库
cp /opt/quant/data/backup/live_quant_20240615.db /opt/quant/data/live_quant.db

# 恢复配置
tar -xzf /opt/quant/data/backup/config_20240615.tar.gz -C /

# 启动
sudo systemctl start quant-live
```

---

## 11. 部署后检查清单（20 项）

**部署完成后，逐项检查：**

```
环境：
[ ] 1. 时区是 UTC
[ ] 2. NTP 同步正常（offset < 10ms）
[ ] 3. 到币安 REST 延迟 < 50ms
[ ] 4. 磁盘剩余 > 10GB
[ ] 5. Python 3.11+ 和依赖已安装

配置：
[ ] 6. .env 权限是 600
[ ] 7. API Key 有合约交易权限
[ ] 8. API Key 有 IP 白名单
[ ] 9. 配置文件校验通过
[ ] 10. 参数与影子报告一致

数据：
[ ] 11. 历史 K 线导入完成（> 40000 根）
[ ] 12. 盘口快照导入完成
[ ] 13. 预热验证通过（500/500）

测试：
[ ] 14. 单元测试全部通过
[ ] 15. 回测验收通过（夏普>1.0，回撤<8%，假信号<15%）
[ ] 16. Preflight 21/21 通过

系统：
[ ] 17. systemd 服务已配置
[ ] 18. 健康检查脚本可运行
[ ] 19. 备份 cron 已配置
[ ] 20. Kill switch 测试通过（触发→恢复）
```

**全部打勾，才算部署完成。**

---

## 12. 三阶段切换 Checklist

### 12.1 回测 → 影子模式

```
[ ] 回测验收全部通过
[ ] 参数与回测一致
[ ] 数据导入完成
[ ] 服务器环境准备完成
[ ] 启动影子模式（前台 10 分钟，确认正常）
[ ] 转后台运行
```

### 12.2 影子模式 → 实盘

```
[ ] 影子模式跑满 72 小时
[ ] 影子验收全部通过
[ ] Preflight 21/21 通过
[ ] 实盘资金已入金（1000 USDT）
[ ] 止损单验证通过
[ ] 撤单验证通过
[ ] Kill switch 验证通过
[ ] 启动实盘（前台 10 分钟，确认正常）
[ ] 转后台运行
```

### 12.3 保护期 → 正常模式

```
[ ] 保护期完成 20 轮
[ ] 无严重异常
[ ] 滑点偏差 < 30%
[ ] 对账偏差 < 1%
[ ] 确认切换到全仓模式（自动）
```

---

## 附录 A：常用命令速查

```bash
# 启动
sudo systemctl start quant-live

# 停止（优雅）
sudo systemctl stop quant-live

# 紧急停止
touch /tmp/quant_live_kill

# 状态
sudo systemctl status quant-live

# 日志
journalctl -u quant-live -f

# 健康检查
/opt/quant/scripts/healthcheck.sh

# 回测
python run_backtest.py --config config --symbol btcusdt --start 2024-03-01 --end 2024-06-01 --seed 42

# 影子模式
python run_shadow.py --config config --symbol btcusdt --min-hours 72

# 实盘
python run_live.py --config config --symbol btcusdt --confirm
```

## 附录 B：关键路径速查

| 内容 | 路径 |
|---|---|
| 代码 | `/opt/quant/quant/` |
| 配置 | `/opt/quant/config/` |
| 数据 | `/opt/quant/data/` |
| 日志 | `/opt/quant/logs/` |
| 报告 | `/opt/quant/reports/` |
| 密钥 | `/opt/quant/.env` |
| 心跳 | `/tmp/quant_live_heartbeat` |
| Kill switch | `/tmp/quant_live_kill` |

---

> **部署手册不是写给“会的人”看的，是写给“三个月后的自己”看的。**
> **每个命令必须可复制粘贴，每个步骤必须有验证方法。**
> **跑起来只是第一步，跑对了才算部署完成。**
> **任何时候出问题，先停，再查，不要边跑边修。**
