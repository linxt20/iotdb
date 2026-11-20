<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Electricity 完整数据映射与验收计划

**目的**：把已校验的 UCI Electricity 完整源转为可重复导入的表模型数据，建立独立真值及全字段核查，再评价并行执行。

**摘要**：保留 140,256 个名义时间点和 370 个客户端的全部 51,894,720 个值点。转换、原始源真值和 SQL 验证分别绑定输入与工具身份，初始实验覆盖真实数值的扫描、分组、排序和有限 CPU 压力。

**结论**：源获取已完成；本计划形成时转换、真值和 SQL 导入均未完成。只有通过转换内容与独立原始源指纹、数据库全字段指纹及完整查询结果核查后，才把相应部署作为计时对象。

## 源与解释

源为 UCI ElectricityLoadDiagrams20112014，DOI `10.24432/C58C86`。sc01 原件：

```text
/root/rivermind-data/iotdb-paper-experiments-20261117/datasets/uci-321-complete-local-stage-20261117/source.zip
```

ZIP SHA256：`f6c4d0e0df12ecdb9ea008dd6eef3518adb52c559d04a9bac2e1b81dcfc8d4e1`。主体 `LD2011_2014.txt` 共 140,256 行，370 个客户端列。UCI API 的实例与特征字段采用转置口径，实际头部和行列校验为本实验依据。

官方说明单位为每十五分钟记录的 kW；换算 kWh 需除以四。未启用客户端的值按零记录，不能删零。葡萄牙墙上时间每天保持 96 个记录；三月变时段为零，十月重复小时的消费被聚合。本实验保留该官方表示，不还原真实 UTC，也不据此计算真实经过时长。

## 表与转换

数据库 `uci_electricity_v4`，表 `electricity_nominal`：

| 列 | 类型与角色 | 内容 |
| --- | --- | --- |
| time | TIMESTAMP TIME | 将源墙上标签按 UTC 解释得到的名义毫秒时间；保持排序与十五分钟间隔 |
| client_id | STRING TAG | 原始 `MT_001` 至 `MT_370` |
| power_kw | DOUBLE FIELD | 小数逗号改为点后，按 binary64 解析；不缩放、不舍入到人工精度 |
| source_row_no | INT64 FIELD | 原文件数据行序号，从一开始 |
| record_present | BOOLEAN FIELD | true；用于明确行存在性 |

每客户端一个可审查的 CSV，共 370 文件。所有源值均需有限，字段或时间形态不符直接失败。输入的非有限、缺列、重复客户端或异常步长不静默替换。写入新的版本目录，保留原件及失败目录。

## 独立真值与内容指纹

转换器输出 CSV 和转换侧逐客户端内容指纹；独立 oracle 再次直接读取原始 ZIP，不读取生成的 CSV。两者使用独立解析过程，核对实际时间、客户端、binary64 位、源行号及存在标志。

每行指纹输入为 UTF-8、制表符分隔的：名义毫秒、客户端 ID、16 位小写 binary64 原始位十六进制、源行号、`true`，以 LF 结束。每客户端按原始时间顺序累计 SHA256；SQL 端要求按时间有序，拒绝 NULL、重复、缺行及类型失配。该指纹与行数共同核查全部导入内容，不以 COUNT/MIN/MAX 代替全字段核查。

初始精确查询真值包括全表及时间前缀的 COUNT/MIN/MAX、按客户端分组、零值分组、首 64 个时点完整有序输出、LIMIT 257、重排和重复列，以及保留相同小窗口输出的全扫描 CPU 压力查询。后者明确属于作用于真实值的压力查询，不冒充代表性业务查询。

SUM/AVG 涉及浮点重排，首轮不采用容忍度不明的“精确相等”判据。后续如加入，必须先冻结参考算法、误差界与拒绝规则，不放宽已选定的精确查询比较器。

## 规模与对照

以全部客户端的一年、两年和四年名义时间前缀形成约 1,300 万、2,600 万及 5,200 万行的查询范围，使用同一完整物理输入。不同年份含不同零值与负载分布，因此不能将其当作分布完全相同的纯规模扩展。

后续四种配置为干净原生上游、v4 固定一人、固定八人和自适应。初值、逻辑 lane、传输开关、预算及实际源布局分别记录。新数据先在隔离服务中验证，不改写已冻结的合成与 Beijing 结论。不同时运行导入、JFR 和性能计时。

## 退出条件与排期

| 工作 | 可核查退出条件 | 预计 |
| --- | --- | --- |
| 完整转换 | 370 文件、51,894,720 行，源和输出 SHA、时间和零值记录完整 | 10–20 分钟 |
| 独立 oracle | 原 ZIP 再解析，全部内容指纹一致，查询真值冻结 | 10–20 分钟 |
| 隔离导入 | 选定 JAR、服务、配置、导入类身份和失败行证据齐全 | 每部署 10–30 分钟，依实际吞吐修订 |
| 全字段核查 | 全部客户端有序指纹、行数及精确类型一致 | 每部署 10–30 分钟 |
| 代表性试跑 | 完整结果与独立 oracle 一致，实际计划及运行组范围明确 | 10–30 分钟 |
| 性能与消融 | 合理预热、匹配块、完整结果、原生计时、实际动作与资源证据 | 正确性关闭后另行安排 |

这些是工作量估计，不是已完成声明。Household 完整源继续保留，待此主数据链路闭环后补 NULL 与单设备时间跨度实验。

来源：sc01 上同源目录的 `acquisition.json`、`remote-verification.json`、`official-metadata.json`；UCI 官方数据说明；r9 的 `planning/本地交换验收与开关实验_20261118.md`。
