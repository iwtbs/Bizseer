# TraceAnalysis
将elasticsearch上的数据导入，将influxdb上的数据导入，将两者数据进行整合
- zipkin收集到的原始数据，包括trace id，每次调用的status code，latency等全部详细信息
- 每个node和docker container的机器性能指标，包括cpu占用率，memory占用，disk读写速度和disk响应时间等
- 每个microservice和node的对应关系
- 根因microservice的标注
- 把原始数据编码成feature
    * 对invocation（A调用B）按end_time排序，每个invocation统计其(http_code, execution_time, cpu, memory)，形成一个(n_invocation, n_features)的序列。
- 将结果保存为2进制文件，代码整理为脚本，python incocation.py --help可查看具体用法
