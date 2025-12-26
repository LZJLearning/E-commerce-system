基于 Docker 的大数据实时处理与分析平台
本项目构建了一个完整的端到端大数据实时处理系统。模拟电商订单数据的实时生成，利用 Kafka 进行消息解耦，实现了双流处理架构：一条链路通过 HBase/Hive 进行数据仓库存储与离线分析，另一条链路通过 ELK (Elasticsearch, Logstash, Kibana) 实现实时监控与大屏展示。

系统架构
数据流向分为两条主要路径：

数仓分析链路： Python (模拟订单) -> Kafka -> Python (批量写入) -> HBase -> Hive (SQL 分析)

实时监控链路： Python (模拟订单) -> Kafka -> Logstash (微批处理) -> Elasticsearch -> Kibana (可视化大屏) / HDFS (冷备份)

核心组件版本
消息队列: Kafka 7.3.0 + Zookeeper 3.8.0

存储与数仓: HBase 1.3 (兼容性优化), Hadoop 3.2.1, Hive 2.3.2

日志与搜索: Elasticsearch 7.12.1, Kibana 7.12.1, Logstash 7.12.1

脚本语言: Python 3.11 (HappyBase, Kafka-Python)