# CycleFlow 机器人样机迭代数据服务

CycleFlow 面向样机研发团队，接收设计修订、部件批次、生产工单和质量试验等独立来源的事件。SQLite 同时保存原始事件身份和派生的业务状态。报表将设计版本、工单、工序耗时、物料批次、试验结果和质量问题连接起来，供团队解释每次迭代的交付效率。

## 运行

依赖版本锁定在 `requirements.txt`。在项目目录运行 `python -m unittest discover -s tests -v` 执行业务测试，`python -m compileall -q cycleflow` 检查源码，`python -m cycleflow.demo` 运行合成数据验收。安装所列依赖后，可设置 `CYCLEFLOW_DB` 为可写的数据库路径，再运行 `uvicorn cycleflow.api:app` 启动 HTTP 服务。命令行帮助为 `python -m cycleflow --db cycleflow.sqlite3 --help`。

演示仅使用临时数据库和本地合成产品。它分两次导入同一清单，验证试验良率，冻结月报，并审计存储的状态链；正常运行无需访问外部服务。

## 事件协议

向 `/events` 提交 `source`、`external_id`、`kind`、带时区的 `occurred_at` 和对象类型的 `payload`。相同事件重放返回既有编号；相同身份提交不同内容返回冲突。缺少前置对象或暂时缺料的事件进入待处理状态，可调用 `/events/{id}/retry` 补齐后重试。其他业务无效事件仍保留拒绝原因。批量接口每次使用稳定的批次编号和完整事件数组；系统核对清单指纹，从已提交的行继续。

事件类型包括 `design.release`、`design.retire`、`lot.receive`、`lot.quarantine`、`job.open`、`job.stage_start`、`job.stage_finish`、`job.scrap`、`plan.create` 和 `test.record`。设计包含产品、版本、部件数量及可选的父版本。部件批次包含料号、供应商和正整数数量。工单绑定一个设计和产量，成功预留物料后依次经过加工、装配和集成。试验计划绑定精确的产品版本，受测数量必须覆盖该工单全部单位。

## 设备占用与排队

多条产线共用的测试设备通过 `equipment.*` 事件记录：`equipment.register` 登记设备（工位、可选归属产线），`equipment.enqueue`/`equipment.withdraw` 记录工单开始排队或放弃，`equipment.acquire`/`equipment.release` 记录占用区间（acquire 自动闭合该工单的排队），`equipment.reserve`/`equipment.rebook` 记录预约时段及其变更，`equipment.down`/`equipment.up` 记录停机区间。设备事件只新增事实，不改写工序时长与试验结果。

占用与停机按 `occurred_at` 区间校验，与事件到达顺序无关：任何与既有占用区间相交的 acquire（包括乱序补录）都会被拒绝，因此同一设备在任何时刻只会分派给一个工单；停机时段内的 acquire 同样被拒绝。预约时段在同一设备上不得重叠，改约会保留被替换的原时段。缺少设备或工单的事件进入待处理状态，补齐后可重试。

`/jobs/{job_id}/wait` 给出单个工单的可归因等待：每段排队被分解为 `downtime`（设备停机）、`contention`（他单占用，列出占用工单）、`reservation_change`（本单预约被推迟）和 `idle_device`（设备空闲、设备侧无法解释）四类，并附占用与预约明细及来源事件编号。`/reports/{product}/equipment` 面向主管：按 ISO 周（跨周区间在周一零点切分）、设计版本和设备拆解瓶颈，设备行的 `down_seconds` 是设备级事实、从不分摊到任何产线，工单明细标记仍在等待交付的工单（`blocking_delivery`），可从汇总逐层追到具体工单。`/equipment/{equipment_id}` 返回设备的占用、排队、停机与预约全量区间。`/quality/audit` 同时校验设备区间互斥、停机配对和预约不重叠。

## 报表与发布

请求 `/reports/{product}?window_start=...&window_end=...` 获取当前视图。时间窗含起点、不含终点，以工单创建时间决定归属。报表提供按版本汇总、良率、工序时长、等待时长、来源事件编号和质量问题。周度、返工分组、瓶颈和供应商视图使用相同的已接受事实。供应商试验归因可以重叠，不能跨供应商直接求和。

向 `/releases` 提交发布编号、产品、时间窗和预览返回的 `expected_fingerprint`。未完成工单或窗口内未解决事件阻止发布。发布后保留原报告和来源事件头；`/releases/{id}/freshness` 比较当前视图与冻结视图，不改写历史。`/quality/audit` 检查事件身份、设计版本链、物料预留、工序顺序、试验记录和发布摘要。
