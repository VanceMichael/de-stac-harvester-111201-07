# 卫星目录·跨日界线采集

面向合作资料接收人员的 Python 采集组件。读取标准条目的观测时间、矩形范围和资产，执行单目录普通经度窗口筛选。
输入样本都是本地合成数据，example 域名只表示来源身份，不需联网访问。

## 安装与运行

使用 Python 3.11，依赖仅为标准库，SQLite 随解释器提供，无需安装第三方包。
从项目根目录执行；演示启动真实的回环 HTTP 来源、处理响应、事务入库后读取实际结果。
每次演示使用独立临时目录，结束后清理；通常两秒内结束，不回放历史输出。

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector demo
```

常规导入保留在本地数据库中，可重复运行；默认路径已忽略，不会进入版本库。

```bash
python3 -m collector import --input fixtures/sample.json --db var/records.sqlite3
python3 -m collector show --db var/records.sqlite3
python3 -m collector serve --db var/records.sqlite3 --port 8080
```

查询服务监听回环地址，`GET /records` 返回数据库中的记录；按 Ctrl-C 停止。

## 联合检索（跨日界线、多目录）

`collector/federation.py` 在单目录导入之外提供多目录联合检索：可能跨越 180° 经线的
查询范围先拆成普通经度窗口，再按 UTC 半开时间范围、集合与必需资产角色逐来源检索。
`collector/scenario.py` 按 `fixtures/catalogs.json` 的描述启动本地合成目录：
primary 的 next 链接要求把 token 合并进原始 POST 请求体，mirror 使用 GET 游标链接，
两者条目 ID 不同但观测相同。

```bash
python3 -m collector federation-demo                                   # 两阶段演示，临时目录
python3 -m collector federate --phase 1 --db var/federation.sqlite3    # 首次检索
python3 -m collector federate --phase 2 --db var/federation.sqlite3    # 续传并追补晚发布资产
python3 -m collector federate --report-only --db var/federation.sqlite3
```

观测身份只由集合、平台、观测时刻与足迹构成，处理版本（processing:version）作为修订
保留；条目 ID 与签名资产地址只是来源内部标识和临时定位，不进入去重键。每个来源、
每个窗口的翻页游标、覆盖状态与隔离记录独立落库：坏条目被隔离后该来源保持不完整，
其他来源已取得的成果不受牵连；中断的检索下次从已存游标续传，已完成的来源重新翻页
以追补晚发布的资产。报告分三部分对外说明：已发现的观测（含修订与来源凭据）、仍缺
少必需资产的观测、未完成的检索区间（含原因），而不是只比较总条数。

## 组件与数据

`collector/domain.py` 负责来源格式和领域校验；`storage.py` 保留原始对象、导入凭据和当前记录。
单次导入在同一事务内提交，异常时整体回滚；相同来源和原文只登记一次凭据。
当前记录以领域标识为键，同键再次导入替换当前观察值，不代表保留了完整修订历史。
`transport.py` 提供请求客户端和本地响应服务；服务按路径依次消费响应脚本，并记录实际请求。
`api.py` 提供只读查询入口。`fixtures/sample.json` 是当前导入器支持的输入。
其他附件描述同领域的合成页面、协议或事件，可以由调用方构造本地来源。
当前响应服务是顺序脚本工具，不自动推断认证、文件或表单业务；领域对象提供对应的本地交互。

## 可复现运行

宿主机或容器均不需要网络。镜像准备好以后可以关闭外部网络再运行上述命令。
仓库内不含真实账号、新闻正文或人员资料；不要将生产凭据写入样本或日志。
