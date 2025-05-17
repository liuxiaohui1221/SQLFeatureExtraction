
## About this repo:

This is a best-effort SQL feature vector extractor which is tailored to the CourseWebsite (MINC) dataset and BusTracker dataset. This is a part of our work towards our paper https://dl.acm.org/doi/10.1145/3442338

CourseWebsite (MINC) dataset is proprietary and cannot be released. The BusTracker dataset is from an earlier work http://www.cs.cmu.edu/~malin199/data/tiramisu-sample/ 

The version of the BusTracker dataset we used is available at
* https://www.dropbox.com/s/umqj1dnc7bhvpcw/BusTracker.zip?dl=0

Pre-created SQL fragment vectors for the BusTracker dataset are available at BusTracker/InputOutput/MincBitFragmentIntentSessions

This github repo has been initially cloned from https://github.com/UBOdin/EttuBench. On the top of their JSQLParser implementation, we have built our SQL fragment embedding vector creator

Our code for SQL fragment extraction includes:

* SQLFeatureExtraction/src/main/java/encoder.MINCFragmentIntent.java
* SQLFeatureExtraction/src/main/java/IntentCreator.java
* SQLFeatureExtraction/src/main/java/encoder.SQLParser.java
* SQLFeatureExtraction/src/main/java/com.clickhouse.SchemaParser.java

The remaining code from https://github.com/UBOdin/EttuBench serves as utility code for our SQL Fragment extraction.

# 数据集预处理：训练数据集与测试数据集

1.单查询one-hot编码：
[APMFragmentIntent.java](src/main/java/sql/encoder/APMFragmentIntent.java)

2.查询窗口(超参数：窗口大小，topN表，topK聚合查询)数据集扩展与one-hot编码：
[APMWindowFragmentIntent.java](src/main/java/sql/encoder/APMWindowFragmentIntent.java)

# 测试数据集综合评测类

1.运行主入口：[DruidQueryJDBCExecutor.java](src/main/java/sql/sender/DruidQueryJDBCExecutor.java)

2.内部包含两个接口辅助类：
1）druid sql查询客户端：[DruidSqlClient.java](src/main/java/sql/sender/DruidSqlClient.java)
2）查询窗口预测客户端：[PredictionClient.java](src/main/java/sql/sender/PredictionClient.java)

备注：依赖的模型预测服务在另一个IncrementalPredictionSQL增量预测SQL python项目，服务类：apm/predictor/app.py