# HiveUDF_Examples

* 创建Hive内表
```
  create table test_table(
    user_id string,
    user_name string,
    hobby array<string>,
    scores map<string,integer>
  )
  row format delimited
  fields terminated by '|'
  collection items terminated by ','
  map keys terminated by ':'
  lines terminated by '\n';
``` 

* 插入数据
```
load data local inpath '/home/hadoop/test.csv' into table test;
```

* 创建Hive外表
```
create external table test_table_external(
	user_id string,
	user_name string,
	hobby array<string>,
	scores map<string,integer>
)
row format delimited
fields terminated by '|'
collection items terminated by ','
map keys terminated by ':'
lines terminated by '\n'
location '/user/hadoop/external_table';
```

* 创建分区表
```
create table partition_table(
	user_id string,
	user_name string,
	hobby array<string>,
	scores map<string,integer>
)
partitioned by (time string)
row format delimited
fields terminated by '|'
collection items terminated by ','
map keys terminated by ':'
lines terminated by '\n';
```
* 插入数据到分区表
```
insert overwrite table partition_table partition (time='202001') select * from test;
```

* 创建分桶表
```
create table bucket_table(
	user_id string,
	user_name string,
	hobby array<string>,
	scores map<string,integer>
)
clustered by (user_name) sorted by (user_name) into 2 buckets
row format delimited
fields terminated by '|'
collection items terminated by ','
map keys terminated by ':'
lines terminated by '\n';
```
* 插入数据到分桶表
```
insert into bucket_table select * from test; 
```
* 查询分桶表
```
select * from bucket_table tablesample(bucket 1 out of 2 on user_name);
```


* HIVE UDF函数操作, 临时注册， 在 hive cli 执行

* add jar
```
add jar hdfs:///udfs/hive-udf-1.0.jar;
```
* create temporary function
```
create temporary function strlen as "com.imooc.hive.udf.StrLen";
create temporary function avg_score as "com.imooc.hive.udf.AvgScore";
```
* use the udf
```
select user_name,strlen(user_name),avg_score(scores) from hive_test.test;
```

* HIVE UDF函数操作, 永久注册，可以在beeline执行

```
create function strlen as 'com.imooc.hive.udf.StrLen' using jar 'hdfs://bigdataarch.vpc.cloudera.com:9000/udfs/hive-udf-1.0.jar';

create function avg_score as 'com.imooc.hive.udf.AvgScore' using jar 'hdfs://bigdataarch.vpc.cloudera.com:9000/udfs/hive-udf-1.0.jar';

drop function strlen;
```



# hive-site.xml

```
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>



<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://localhost:3306/metastore?useSSL=false</value>
    <description>the URL of the MySQL database</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.jdbc.Driver</value>
    <description>Driver class name for a JDBC metastore</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>1qaz@WSX</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/hive/warehouse</value>
  </property>
  <property>
<!--    <name>hive.server2.enable.doAs</name>-->
    <name>hive.server2.enable.impersonation</name>
    <value>true</value>
    <description>Set this property to enable impersonation in Hive Server 2</description>
  </property>
  <property>
    <name>hive.metastore.execute.setugi</name>
    <value>true</value>
    <description>Set this property to enable impersonation in Hive Metastore</description>
  </property>
  <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
  </property>
  <property>
    <name>hive.users.in.admin.role</name>
    <value>hadoop</value>
  </property>
  <property>
    <name>hive.metastore.authorization.storage.checks</name>
    <value>true</value>
  </property> 
  <property>
    <name>hive.secutiry.authorization.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.secutiry.authorization.createtable.owner.grants</name>
    <value>ALL</value>
  </property>
  <property>
    <name>hive.secutiry.authorization.task.factory</name>
    <value>org.apache.hadoop.hive.ql.parse.authorization.HiveAuthorizationTaskFactoryImpl</value>
  </property>
  <property>
    <name>hive.sementic.analyzer.hook</name>
    <value>com.imooc.hive.security.HiveAdmin</value>
  </property> 
</configuration>
```

## Hive Install Init
```
schematool --dbType mysql --initSchema
```
