# DataSkewness
Simulating and Solving Data Skewness.

# Spark、Hive模拟数据倾斜

采用 `Spark On Hive` 的集群方式，下面是一些技术所扮演的角色：

- HDFS：存储数据

- YARN：资源调度，包括HDFS与Spark中的Master、Worker

- Hive：负责管理元数据

- Spark：负责进行查询计算，YARN负责Spark需要使用的资源调度

- MySQL：存储元数据

## 1 Spark Yarn集群

这里使用YARN的方式部署Spark，选择YARN作为Spark集群资源的调度方式。

具体步骤如下：

1. 在 `hadoop102` 上安装Spark和Scala以及对应版本的JDK，同时配置好环境变量；
2. 修改Spark集群配置，在/opt/moudle/spark/conf目录下，修改 ./slaves.template 为 ./slaves，同时在该配置文件中将`localhost`替换成集群中的主机名：
   - hadoop102
   - hadoop103
   - hadoop104
3. 修改 /opt/moudle/spark/conf/spark-env.sh.template 为 ./spark-env.sh；新增内容：

```shell
export SPARK_DIST_CLASSPATH=$(/opt/moudle/hadoop/bin/hadoop classpath)

export SPAEK_MASTER_HOST=hadoop102

export SPARK_MASTER_PORT=7077

export SPARK_MASTER_WEBUI_PORT=8080

export SPARK_WORKER_MEMORY=1g

export SPARK_WORKER_CORES=1

export SPARK_WORKER_INSTANCES=1

export HADOOP_CONF_DIR=/opt/moudle/hadoop/etc/hadoop
```

4. 将修改好的 spark 使用 xsync 分发到 hadoo103、hadoop104主机中，并修改 /opt/moudle/spark/conf/slaves，将`hadoop102`删除只保留剩下两台主机：

   - hadoop103

   - hadoop104

5. 以上就完成了Spark集群规划，hadoop102作为Master节点，hadoop103、hadoop104作为Worker节点

6. 启动集群进行测试：

   ```bash
   ## 先启动Hadoop集群，主要是HDFS和Yarn
   /opt/moudle/hadoop/sbin/start-all.sh
   
   ## 然后在 hadoop102启动Master进程
   /opt/moudle/spark/sbin/start-master.sh
   
   ## 在hadoop103、hadoop104启动Worker进行
   /opt/moudle/spark/sbin/start-slaves.sh
   ```

7. 启动成功之后，可以使用 jpsall 命令查看三台机器进行情况

8. 也已通过下面的网址进行查看：

   ```text
   1 hadoop102:9870 【hadoop 的WEB UI界面】
   
   2 hadoop103:8088 【yarn 的 WEB UI 界面】
   
   3 hadoop102:8080 【spark 集群资源的 WEB UI界面】
   
   4 hadoop102:4040 【spark 应用程序界面，app执行过程的WEB UI 界面】
   ```

> Spark 8080 端口和 4040 端口分别用于不同的 Spark Web 界面和功能：
>
> 1. **8080 端口**：
>
>    - 8080 端口通常用于 Spark 集群的 Web 界面，它展示了有关 Spark 应用程序、集群状况和资源使用情况的信息。
>    - 8080 端口上运行 Spark 的集群管理界面，您可以通过浏览器访问该界面来查看 Spark 应用程序的运行情况、监视集群资源使用情况、查看作业历史和掌握 Spark 应用程序的详细信息。
>
> 2. **4040 端口**：
>
>    - 4040 端口用于 Spark 的应用程序界面，也称为 Spark Application UI。
>    - 当您提交一个 Spark 应用程序后，Spark 将为该应用程序分配一个唯一的 4040 端口（也可以是其他可用的端口，例如 4041、4042 等）。
>    - 通过访问 `http://your_spark_driver_node:4040`，您可以查看特定 Spark 应用程序的详细信息，包括任务执行情况、作业计划、阶段信息、RDD 缓存等。
>    - Spark Application UI 是一个非常有用的工具，可用于调试和监视 Spark 应用程序的性能和执行。
>
> 请注意，这些端口号是默认的设置，您可以通过配置文件或启动参数更改它们，以适应特定的部署需求。在生产环境中，通常会配置防火墙规则以限制对这些端口的访问，以提高安全性。

##  2 Spark SQL整合Hive

配置好Spark集群后，需要配置SparkSQL和Hive的整合，目的是使用SparkSQL来进行数据的查询和分析计算，Hive仅作为元数据的管理角色。

1. 启动 Hive 的 metastore 服务，并将 /opt/moudle/hive/conf/hive-site.xml 文件拷贝到 /opt/moudle/spark/conf/ 目录下然后添加：

   ```xml
   <property>
       <name>hive.metastore.uris</name>
       <value>thrift://hadoop102:9083</value>
   </property>
   ```

   > hive-site.xml 文件中的内容是基于 Hive On Spark 的方式来进行的，如果执行 hive查询的时候，依旧可以使用 Spark 来进行查询加速，而不是使用 Hadoop 中的Map Reduce.

2. 因为Hive 时使用 MySQL 作为元数据存储媒介，所以需要将 mysql-connector-java-5.1.32.jar 拷贝到 spark 存放 jar 包的目录下，同时在 spark on yarn 的运行模式下，还需要拷贝一份到 HDFS 上，路径就是存储 spark-jar包的路径：/spark-jars

3. 分别启动 Hadoop 集群和 metastore 服务：

   ```bash
   start-all.sh
   
   nohup /opt/moudle/hive/bin/hive --service metastore 2>1& &
   ```

4. 使用 `spark-sql`客户端和 `hive` 客户端进行测试:

   ```bash
   /opt/moudle/spark/bin/spark-sql --master local[*]
   
   /opt/moudle/hive/bin/hive
   ```

5. 先使用 spark-sql 来创建一个数据库：test1，然后使用 hive进行查询，看是否存在数据库 test1

   ```shell
   spark-sql> create database if not exists test1;
   
   hive> show databases;
   ```

   不出意外可以在 hive 窗口看到 spark-sql 创建的数据库；

6. 再使用 hive 创建表 student，并向表中添加数据；在 spark-sql 中进行查询，看是否有表并且有数据

   ```shell
   hive> create table if not exists student(id int, name string);
   hive> insert into table student values(1,'dasen');
   
   spark-sql> select * from test1.student;
   ```

   不出意外可以看到数据：1 -- dasen

7. 最后使用 spark-shell 进行测试，进入spark-shell 客户端，然后执行下面代码，看是否存在数据：

   ```scala
   import org.apache.spark.sql.SparkSession
   
   val spark = SparkSession.builder()
   			.appName("Hive Query")
   			.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
   			.enableHiveSupport()
   			.getOrCreate()
   			
   val result = spark.sql("select * from test1.student")
   result.show()
   ```

   最后出现结果：

   ![image-20230916223838486](D:\Typora_保存图片\image-20230916223838486.png)

表明已经将 Spark SQL和 Hive 整合成功。



## 3 搭建数据倾斜场景

### 困境

本demo使用百度7/17-7/23一周的全网数据中心的报警数据进行模拟，同时按照各模组报警数量的不同，生成了大量的重复数据，并将整体数据量扩容到5000,0000 左右，后续根据情况再进行增加或者删除。

在进行测试之前，写了一个wordcount程序，使用`sbt`打包后，在集群中使用 `spark-submit` 命令提交到集群中进行运行。

此处有一个问题，就是即便我使用了 `--master yarn`与`--deploy-mode cluster`两个参数，但是在Spark Job WEB UI界面依旧看不到 Spark 执行整个任务的流程。后面我使用 `spark-sql`却可以看到整个 job 的执行情况。如下图：

![image-20230917212651974](D:\Typora_保存图片\image-20230917212651974.png)

<font color='green'>也就是我无法在本地 idea 中编写程序，然后远程提交到 spark 集群进行任务运行。时间比较紧先mark一下，目前先使用 spark-sql 客户端来进行数据分析计算以及数据倾斜模拟与解决。</font>

### 搭建

首先第一步，我在`mysql`中将` 17-23` 号的数据进行了一下简单的处理，摒弃掉了部分无用的字段，然后建了一张新表进行存储，便于后续增加数据量或者减少数据量的操作，下面是sql语句：

```sql
CREATE TABLE
IF NOT EXISTS ods_idc_warrings (
	warring_id VARCHAR (255),
	-- 指定长度，例如255
	message_source VARCHAR (255),
	-- 指定长度，例如255
	message_level VARCHAR (50),
	-- 指定长度，例如50
	message_type VARCHAR (50),
	-- 指定长度，例如50
	message_start_time DATETIME,
	-- 指定长度，例如20
	message_end_time DATETIME message_duration_time VARCHAR (20),
	-- 指定长度，例如20
	message_storage_time DATETIME,
	-- 指定长度，例如20
	message_delay_time VARCHAR (20) -- 指定长度，例如20
);

-- 向表中插入原始数据
INSERT INTO ods_idc_warrings (
	warring_id,
	message_source,
	message_level,
	message_type,
	message_start_time,
	message_end_time,
	message_duration_time,
	message_storage_time,
	message_delay_time
) SELECT
	warring_id,
	message_source,
	message_level,
	message_type,
	message_start_time,
	message_end_time,
	message_duration_time,
	-- 列名映射
	message_storage_time,
	message_delay_time
FROM
	ods_true_montior_warrings_etl_17_23;
```

因为没有设置主键，所以可以重复的增加大量重复数据。添加的时候直接使用 `INSERT INTO ... `即可。



其次第二步，我使用` Hive `创建了用来存储报警数据的表，其实也可以直接使用` spark-sql` 来进行创建，作为存储报警数据的原始数据表。因为这个 `demo` 主要的目的是模拟数据倾斜并进行解决，所以没有搭建数据仓库，而只是建立了`ODS`层， 以及最终的 `ADS` 层。直接从 `ODS` 拿数据然后进行统计分析，查看任务运行的情况。后面再细说分析指标。下面是创建`Hive`表：

```sql
DROP TABLE  IF EXISTS  idc_warring.ods_idc_warrings;
CREATE TABLE IF NOT EXISTS idc_warring.ods_idc_warrings (
    warring_id STRING,
    message_source STRING,
    message_level STRING,
    message_type STRING,
    message_start_time STRING,
    message_end_time STRING,
    message_duration_time STRING,
    message_storage_time STRING,
    message_delay_time STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
```



第三步，我手动的将 `mysql` 中的数据导入到 `HDFS` 中，然后作为 `Hive` 表的数据。先将` mysql` 数据导出为` txt` 文本文件，然后将文本文件上传到` HDFS`中，紧接着使用 `LOAD DATA `命令将数据加载覆盖到创建好的`Hive`表中，如下命令：

```shell
hadoop dfs -put ./ods_idc_warrings.txt /origin_data/idc/

LOAD DATA INPATH '/origin_data/idc' OVERWRITE INTO TABLE idc_warring.ods_idc_warrings;
```

> 这里有个需要注意的地方，就是 `INPATH` 只写到路径即可，不用写具体的文本文件

## 4 数据倾斜模拟

### 如何判断出现数据倾斜
