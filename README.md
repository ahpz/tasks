# tasks
python master-worker 多进程调度
任务调度模块设计
一 背景
目前线上的离线任务采用kafka异步消费消息执行任务, 执行结果存储Redis中
- 离线任务的吞吐率以及水平拓展能力 受到Kafka 的partition的数目影响（新增partition需要重启集群)
- 离线报表历史下载记录Redis 虽然可以持久化存储，但是不利于查询,统计
- 公司Redis 目前稳定性不少很高。造成下载任务的抖动性失败
- Kafka 单次消费存在最大时间限制(5min) 超时会出现rebalance，导致任务重复消费，降低吞吐率

二 设计目标
2.1 实现功能
- 任务执行吞吐率水平拓展
- 历史任务的日志记录查询和统计
- 大耗时长的大任务的支持，保证下载，不保证延时
- 不同类别任务（不同耗时)相互隔离，降低影响
2.2 性能目标
- QPS 增大，任务消费能力水平拓展，降低等待时长

三 总体设计
3.1 系统架构图
注意步骤6：查询任务结果可返回（PENDING/SUCCESS/FAILED) 三种状态
不同的业务方任务区分处理，同一个业务方任务根据任务类型可以做进一步区分和空闲worker监控


 
3.2 离线任务核心步骤流程



采用master-worker 多进程模式, master 进程负责分配任务put queue. worker进程负责 get from queue,执行任务
3.2 核心类UML图

3.3 任务状态机

3.4 任务数据库表
CREATE TABLE `task_log` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `log_id` varchar(64) NOT NULL DEFAULT '' COMMENT '日志ID',
  `advertiser_id` bigint(20) NOT NULL COMMENT '广告主ID',
  `template_id` bigint(20) NOT NULL COMMENT '模板ID',
  `core_user_id` bigint(20) NOT NULL COMMENT '用户ID',
  `origin` tinyint(3) NOT NULL COMMENT '来源 1实时任务 2定时任务',
  `stat` tinyint(3) NOT NULL COMMENT '0:任务新建 1:任务提交 2:任务返回 3:本地拼装',
  `retry_cnt` tinyint(3) NOT NULL COMMENT '任务重试次数',
  `send_cnt` tinyint(3) NOT NULL COMMENT '任务发送次数',
  `download_cnt` tinyint(3) NOT NULL COMMENT '任务下载次数',
  `cluster` varchar(16) NOT NULL COMMENT '集群',
  `hostip` varchar(16) NOT NULL COMMENT 'ip',
  `podname` varchar(64) NOT NULL COMMENT '实例名称',
  `data` text,
  `isdel` tinyint(4) NOT NULL COMMENT '0 正常 1 删除',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `heartbeat_time` datetime NOT NULL COMMENT '心跳时间',
  `modify_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_log_id` (`log_id`),
  KEY `idx_stat` (`stat`),
  key `idx_modify_time` (`modify_time`),
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
log_id: 任务请求参数+时间生成的hash值

四 模块设计
4.1  Worker 类
4.1.1 功能描述
- 内存队列get 任务ID
- 数据库查询任务 by ID 检查stat==1 and ip={ip} and host={host} and cluster={cluster} //可能由于长时间未执行 被其他集群或者机器执行，（如果重新分配到自己 则第二次执行 状态会不一致) 
- 写入ID到队列
4.1.1 流程图

4.2 Master 类
4.2.1 功能描述
- 创建worker 类子进程, 启动进程
- 监听子进程, 负责拉起意外终止的进程
- 监听用户信号,重启,停止服务 (修改全局变量)
- 分配任务到内存队列
4.2.2 流程图

4.3 Service 类
4.3.1 功能描述
- 服务类基类(worker 和 master 均继承此类)
- 全局变量以及master成员类的管理
4.4 状态重置脚本
4.4.1 功能描述
- 兜底人工介入 重新触发某个任务重新执行 (可能前期一些未知问题导致)
4.5 心跳定时器
4.5.1 功能描述
   worker 进程设置定时信号处理脚本 默认心跳时间30s (tcc 配置), 更新task 表心跳时间heartbeat_time
   master 进程设置定时信号处理脚本 2*默认心跳时间30s (tcc 配置) 如果任务始终未执行，则乐观锁更    新 stat=0 where stat={stat} 
五 风险预估
5.1 已知和可预知的风险
任务耗时: 30s-3min 目前已知最大的dpa 商品+creative_id 耗时 20min
假定任务平均耗时1min
任务吞吐率: tce 实例 * worker /3*60

5.2 监控完善
 数据量的增大，任务平均耗时增大，任务心跳的增大，
附录

调度器调研 
https://www.cnblogs.com/CodeWithTxT/p/11370215.html golang 调度某些
实现 golang 的类似内核调度系统


