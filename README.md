# cdhAgent
CDH 上 Hadoop 服务角色异常退出自动拉起 

依赖 Clouder Manager API 去获取到 Hadoop 角色的异常退出信息，然后根据其提供的 API 重启即可

1分钟检测一次，10分钟内只自动重启一次。10分钟时间够运维人员登上服务器查看信息了，也减少频繁重启
