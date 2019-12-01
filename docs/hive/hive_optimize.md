# SQL优化

## 执行过程优化

### 1、尽量不使用笛卡尔乘积
笛卡尔积数据量大，被分配的reduce任务个数少，耗时严重  
以下两种形式的SQL会导致全表笛卡尔积：  

```sql
select * from A, B where A.key = B.key and A.key > 10;  
select * from A join B where A.key = B.key and A.key > 10;
```

正确写法

```sql
select * from A join B on A.key = B.key where A.key > 10;
```

多个数据表连接时，也要注意将on内写

```sql
A join B join tablec join ... on A.col1 = B.col2 and ...
```
改为

```sql
A join B on A.key = B.key and A.key > 10 join C on  A.key = C.key join  ... on  ...
```

### 2、尽量使用map join
小表join大表的时候，使用mapjoin把小表放到内存中处理，  
语法很简单只需要增加 /*+ MAPJOIN(pt) */ ，把需要分发的表放入到内存中

```sql
SELECT 0_test.pv, 1_test.pv 
FROM 0_test
JOIN 1_test
ON 0_test.cookieid = 1_test.cookieid
```
改为
```sql
SELECT /*+ mapjoin(0_test)*/ 0_test.pv, 1_test.pv
FROM 0_test
JOIN 1_test
ON 0_test.cookieid = 1_test.cookieid
```
考虑到map join比较消耗内存，不支持多表连接  
在开始执行hash map join之前，需要对部分参数进行设置。主要包括：

- hive.mapjoin.cache.numrows  
hash map join会优先将计算所需的小表键值对保存在内存容器中，剩下的键值对将被保存到硬盘。该参数决定保存在内存中的记录条数，系统默认设置该参数为500000。该条数越多，所需的内存越多，则需要将mapred.child.java.opts设的越大。TDW中的默认hash分区数为500，因此，用户可以根据小表的行数算出将该参数设为多少较合适。示例如下：
 set hive.mapjoin.cache.numrows=3000000

- mapred.child.java.opts  
该参数设置map任务和reduce任务可用的虚拟机最大内存。由于map join是一个比较消耗内存的操作，而默认的内存大小为1G，即1024M，因此，在执行上述hash map join查询命令之前最好先用set命令设置内存大小。根据经验数据，在列数不多的情况下，一百万条记录所需的内存容器大小约为500~700MB.因此，如果在内存中缓冲的数据条数为300万，则将该参数设为3072M较为稳妥。原则上，mapred.child.java.opts的大小不要超过4096M。如果单个map任务要处理的条数较多，超过内存限制，则可以根据可用内存大小设置hive.mapjoin.cache.numrows，超出部分将被放入硬盘文件缓存。示例如下： set mapred.child.java.opts=-Xmx4096M；

- Sorted.Merge.Map.Join  
TDW中提供了两种hash map join算法，两者的区别是一种算法使用hash map作为内存容器，而后一种使用数组（sorted list）作为内存容器。前者是TDW中的默认hash map join算法，后者的速度更快，对内存的需求更少。但是，后者要求小表的每个hash分区中的数据必须是有序的，且如果小表的hash分区是二级分区的话不能有多个一级分区参与计算。如要使用基于sorted list的算法,set Sorted.Merge.Map.Join = true；

### 3、及时掌握表的数据情况

```sql
# 查看每个分区数据量大小
show rowcount extended A
# 查看每个分区数据大小，单位是字节
show tablesize extended  A 
```

| table_partition | size |
| --------------- | ---- |
| pri partitions  | NULL |
| default         | 10   |
| row count:      | 10   |

### 4、全局distinct、count

```sql
SELECT COUNT(DISTINCT key)
FROM A
WHERE key >= 20121001
```
改为

```sql
SELECT COUNT(key)
FROM (        
    SELECT DISTINCT key AS key
    FROM A
    WHERE key >= 20121001
)
```
虽然修改后的方法繁琐一些，但是执行效率高很多，前者只会有一个reduce的任务，在数据量大的时候问题比较严重

### 5、全局的group by， count

```sql
SELECT count(1) FROM A;
```
改写成

```sql
SELECT pt,count(1) 
FROM A 
GROUP BY pt;
```

### 5、减少使用exists 和 not exists
exists内部是join实现的，
- NOT EXISTS改写为left join加is null

```sql
SELECT a
FROM A
WHERE
    a.key not exists
	(# exists 后面的子查询返回的是一个boolean值，和select查询的出来的字段没有任何关系，只要where条件通过即可
		select 1 from B where A.col1=B.col2 and B.col3=… and…
	)
```
改写为
```sql
SELECT a
FROM A 
LEFT JOIN B 
ON A.col1=B.col2 and B.col3=… and …
Where … and b.col2 is null
```
- exists改写成left simi join（左主表连接，只能查出左表的字段）

```sql
SELECT … 
FROM A
WHERE 
	… AND exists
	(
		SELECT 1 
		FROM B 
		WHERE A.col1=B.col2 AND B.col3=… AND…
    )
```
改写
```sql
SELECT … 
FROM A 
left semi join B 
on A.col1=B.col2 AND B.col3=… AND …
WHERE …
```
改写后，根据各表的数据量大小调整join顺序，保证小表连接大表，或者使用map join

### 6、把控MAP REDUCE数量
原则上需要因地制宜，根据具体任务实地调优，让每个map处理合适的数据量、配置合理的map、reduce个数。
- map端  
使大数据量利用合适的map数；使单个map任务处理合适的数据量  
1. 先搞清楚map数怎么来的  
根据hadoop file split方法来切割map数，针对每个文件，切割成map数为（文件大小/文件块大小 + 1），如文件块为64M，输入为两个文件，70M和20M，将切割成3个map（64M + 6M + 20M）

2. 调整map数  
map数太多，每个map处理的数据量小，则大量的时间浪费在map的启动和初始化上；
所以在执行任务之前先对map数归并一下
```
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
```
map数太少，则资源浪费，单个map处理时间太长。所以强行的将map数加大
```
set mapred.reduce.tasks=10;
```

- reduce端   
使大数据量利用合适的reduce数；使单个reduce任务处理合适的数据量
1. 先搞清楚reduce数怎么来的
```
hive.exec.reducers.bytes.per.reducer（每个reduce任务处理的数据量，默认为1G）
hive.exec.reducers.max（每个任务最大的reduce数，默认为999）
```
所以，如果map的输出有5.5G，将只有6个reduce。  
有以下几种情况会导致只有一个reduce
a) map端的输出  
b) 没有group by的汇总，比如把select pt,count(1) from A group by pt; 写成 select count(1) from A;  
c) 用了Order by  
d) 有笛卡尔积

2. 调整reduce个数  
修改每个reduce处理的数据量
```
set hive.exec.reducers.bytes.per.reducer=500000000; （500M）
```
强制指定reduce个数
```
set mapred.reduce.tasks = 15;
```

### 7、避免不同的数据结构发生对比
string和bigint比较的时候，reduce会发生数据倾斜，请先cast强制转换格式
```sql
SELECT 
	t2.inewlevel8 as snewlevel8,t1.skey,t1.svalue
from (
    SELECT suin from tA
)t1 
join (    
    SELECT cast(iuin as STRING) as suin
    FROM tB
) t2 
on (t1.suin = t2.suin)
```

## 大牛博客参考
[控制hive任务中的map数和reduce数](http://lxw1234.com/archives/2015/04/15.htm)  
[hive计算map数和reduce数](http://blog.csdn.net/dxl342/article/details/77886551)

> @ 学必求其心得，业必贵其专精
> @ WHAT - HOW - WHY
> @ 不积跬步 - 无以至千里