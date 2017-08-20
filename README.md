# meituan-sparkproject

本项目使用Java语言编写，分为离线部分和实时部分；

离线部分：使用Spark Core或者Spark Streaming技术从MySQL中读取原始数据，进行数据分析处理，得到处理结果再导出到MySQL中

实时部分：使用Spark Streaming技术从Kafka集群中读取日志数据，进行数据分析处理，得到的处理结果导出到MySQL中
