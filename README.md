#spark 监控
##介绍
利用spark rest API监控applications信息，jobs信息，stages信息，executors信息等
##目的
监控与告警可以及时发现问题和解决问题
##部署
执行命令：

$git clone https://github.com/leno1001/spark_monitor.git

$python spark_monitor/monitor/main.py
##环境
1. 有python 环境
2. 安装第三方模块influxdb: pip install influxdb
3. 安装第三方模块requests：pip install requests