# Hive Quick Start

## 一、安装&启动
```shell
brew install hive
brew install mysql
mysql.server start
或者
brew services start mysql (启动)
brew services stop mysql (停止)
  mysql -u root -p
  CREATE DATABASE hive;
  use hive;
  create user 'hive'@'localhost' identified by 'hive'；
  flush privileges；
  grant all privileges on hive.* to hive@localhost;
  flush privileges;
hive --service metastore &
hive --service hiveserver2 &
hive shell
```
### 问题
1. Missing Hive Execution Jar: /usr/local/Cellar/hive/1.2.1/lib/hive-exec-*.jar  
   【解决】修改下HIVE_HOME为export HIVE_HOME=/usr/local/Cellar/hive/1.2.1/libexec，问题不再现
2. Could not create ServerSocket on address 0.0.0.0/0.0.0.0:9083  
   【解决】hive metastore进程已经有启动过了，kill掉  
3. Exception in thread "main" java.lang.RuntimeException: java.lang.IllegalArgumentException: java.net.URISyntaxException: Relative path in absolute URI: ${system:java.io.tmpdir%7D/$%7Bsystem:user.name%7D  
   FAILED: IllegalArgumentException java.net.URISyntaxException: Relative path in absolute URI: file:./$HIVE_HOME/iotmp/982c62b1-c776-4e95-9c91-a0b30e3b057b/hive_2016-06-28_17-58-00_856_4520712065933347856-1  
【解决】  
```
    <property>  
    <name>hive.exec.scratchdir</name>  
    <value>/tmp/mydir</value>  
    <description>Scratch space for Hive jobs</description>  
    </property>  
```
    把所有的${system:java.io.tmpdir}/${system:user.name}改为绝对路径
4. [Warning] could not update stats.  
【解决】  
```
<property>  
    <name>hive.stats.autogather</name>  
    <value>false</value>  
</property>  
```
5. Failed with exception Unable to alter table. For direct MetaStore DB connections, we don't support retries at the client level.  
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.MoveTask  
【解决】  
```
<property>  
    <name>hive.insert.into.multilevel.dirs</name>  
    <value>true</value>  
</property> 
```
## 二、HQL语言
- 建表等  
```sql
CREATE TABLE test_hive (a int, b int, c int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';  
select * from test_hive;  
desc test_hive;  
CREATE TABLE test_hive_copy AS SELECT * FROM test_hive;  
CREATE TABLE test_hive_copy LIKE test_hive;
```

- 导入数据  
```
LOAD DATA LOCAL INPATH '/usr/local/Cellar/hive/1.2.1/libexec/examples/files/test_hive.txt' OVERWRITE INTO TABLE test_hive;  --本地文件  
LOAD DATA INPATH '/hhf/test_hdfs.txt' INTO TABLE test_hive; --hdfs文件  
>  导入后hdfs内的数据就没了，导入本地的文件后会上传到“hive.metastore.warehouse.dir”配置中
```

- 导出数据  
```
insert overwrite local directory '/usr/local/Cellar/hive/1.2.1/libexec/examples/test_export_data'
select * from test_hive;
```

- 数据类型  
原SQL数据类型都支持
新加了几个  
Struct{a INT; b INT}  
Map 键值对
Array['a', 'b', 'c'](也可以使用索引访问内部数据)

- 操作大致一样  
order by 修改为 sort by

- 普通查询：排序，列别名，嵌套子查询  
> from(  
> select a,b,c as c2 from test_hive  
> )t  
> select t.a,t.c2  
> where a>2111;

- 连接查询：JOIN..ON..
> select t1.a,t1.b,t2.a,t2.b  
> from te  
> from test_hive t1 join test_hive_copy t2 on t1.a=t2.a  
> where t1.a>2111;  

- 聚合查询：count, avg
> select count(*), avg(a) from test_hive;

- 聚合查询：GROUP BY, HAVING
 > SELECT avg(a),b,sum(c) FROM test_hive GROUP BY b,c HAVING sum(c)>30

## 三、Hive交互式模式
- quit,exit:  退出交互式shell  
- reset: 重置配置为默认值
- set <key>=<value> : 修改特定变量的值(如果变量名拼写错误，不会报错)
- set :  输出用户覆盖的hive配置变量
- set -v : 输出所有Hadoop和Hive的配置变量
- add FILE[S] *, add JAR[S] *, add ARCHIVE[S] * : 添加 一个或多个 file, jar, archives到分布式缓存
- list FILE[S], list JAR[S], list ARCHIVE[S] : 输出已经添加到分布式缓存的资源。
- list FILE[S] *, list JAR[S] *,list ARCHIVE[S] * : 检查给定的资源是否添加到分布式缓存
- delete FILE[S] *,delete JAR[S] *,delete ARCHIVE[S] * : 从分布式缓存删除指定的资源
- ! <command> :  从Hive shell执行一个shell命令
- dfs <dfs command> :  从Hive shell执行一个dfs命令
- <query string> : 执行一个Hive 查询，然后输出结果到标准输出
- source FILE <filepath>:  在CLI里执行一个hive脚本文件

## 四、与HDFS的交互
| 类型     | 描述                      | 示例  |
| -------- | ------------------------- | ----- |
| TINYINT  | 1个字节（8位）有符号整数  | 1     |
| SMALLINT | 2字节（16位）有符号整数   | 1     |
| INT      | 4字节（32位）有符号整数   | 1     |
| BIGINT   | 8字节（64位）有符号整数   | 1     |
| FLOAT    | 4字节（32位）单精度浮点数 | 1.0   |
| DOUBLE   | 8字节（64位）双精度浮点数 | 1.0   |
| BOOLEAN  | true/false                | true  |
| STRING   | 字符串                    | "xia" |

```sql
## 创建hive表
hive -e "create EXTERNAL table article_gmp (article_id string, product_id string, click double, request double, gmp double, last_update_time string, total_request string, language string) partitioned by (date string, min string) row format delimited fields terminated by '\t' LOCATION '/inveno-projects/offline/article-gmp/data/article-gmp/history/';"
create EXTERNAL table test_uid (uid string) row format delimited fields terminated by '\t' LOCATION '/user/haifeng.huang/profile';

## 为hive表创建分区
hive -e "alter table article_gmp add partition (date='20160917', min='2350') location '/inveno-projects/offline/article-gmp/data/article-gmp/history/20160917/2350/'"

## 展示分区数
show partitions article_gmp

## 简单查询
select * from article_gmp where date='20160917' and min='2350' and article_id="1010497577" and last_update_time is not null;
select distinct  get_json_object(json_string,'$.uid') from impression_reformat where date=20161007 and get_json_object(json_string,'$.article_impression_extra.content_id')=1021004753 

## 批量导入分区
/usr/lib/hive/bin/hive -e "alter table article_gmp add partition (date='20160921', min='0310') location '/inveno-projects/offline/article-gmp/data/article-gmp/history/20160921/0310/' partition (date='20160921', min='0300') location '/inveno-projects/offline/article-gmp/data/article-gmp/history/20160921/0300/' partition (date='20160921', min='0250') location '/inveno-projects/offline/article-gmp/data/article-gmp/history/20160921/0250/' partition (date='20160921', min='0240') location '/inveno-projects/offline/article-gmp/data/article-gmp/history/20160921/0240/' partition (date='20160921', min='0230') location '/inveno-projects/offline/article-gmp/data/article-gmp/history/20160921/0230/' partition (date='20160921', min='0220') location '/inveno-projects/offline/article-gmp/data/article-gmp/history/20160921/0220/'"
```

> @ WHAT - HOW - WHY  
> @ 不积跬步 - 无以至千里  
> @ 学必求其心得 - 业必贵其专精