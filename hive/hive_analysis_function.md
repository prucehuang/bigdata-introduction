[toc]

# 一、数据准备

```
-----------------------------------
--- 创建表
-----------------------------------
CREATE TABLE IF NOT EXISTS 0_test (
	cookieid string,
	createtime string, 
	pv INT
) COMMENT '测试分析函数表' 

-----------------------------------
--- 插入数据
-----------------------------------
SHOW CREATE TABLE 0_test;

INSERT INTO 0_test VALUES ('cookie1', '2015-04-10', 1) 
INSERT INTO 0_test VALUES ('cookie1', '2015-04-11', 5);
INSERT INTO 0_test VALUES ('cookie1', '2015-04-12', 7);
INSERT INTO 0_test VALUES ('cookie1', '2015-04-13', 3);
INSERT INTO 0_test VALUES ('cookie1', '2015-04-14', 2);
INSERT INTO 0_test VALUES ('cookie1', '2015-04-15', 4);
INSERT INTO 0_test VALUES ('cookie1', '2015-04-16', 4);
INSERT INTO 0_test VALUES ('cookie2', '2015-04-14', 1);
INSERT INTO 0_test VALUES ('cookie2', '2015-04-15', 2);
INSERT INTO 0_test VALUES ('cookie2', '2015-04-16', 3);

select * from 0_test;
```


# 二、统计窗口函数
窗口，也就是分组的概念，统计窗口函数就是分组后的统计函数。  

分组的关键字是PARTITION BY，相当于GROUP BY，如果没有加上ORDER BY语句，统计的就是全组的数据，有ORDER BY的话就是根据排序字段的一个累积统计，详见下面的PV_\*\_1 和 PV_\*\_2  

常用的使用场景比如：统计每个组内某个字段最大的记录、统计每个组内某个字段在全组中的权重占比、统计每个组内某个字段的累积值等等
```
-----------------------------------
-- SUM ( [ DISTINCT ] expr )OVER ( [query_partition_clause] [order_by_clause] )
-- AVG ( [ DISTINCT ] expr )OVER ( [query_partition_clause] [order_by_clause] )
-- MAX ( [ DISTINCT ] expr )OVER ( [query_partition_clause] [order_by_clause] )
-- MIN ( [ DISTINCT ] expr )OVER ( [query_partition_clause] [order_by_clause] )
-- RATIO_TO_REPORT  ( expr )OVER ( [query_partition_clause] [order_by_clause] )
-----------------------------------
SELECT 
	cookieid,
	createtime,
	pv,
	-- 默认为从起点到当前行的累加
	SUM(pv) OVER(PARTITION BY cookieid ORDER BY createtime) AS pv_sum_1,
	-- 真个分组内的累加
	SUM(pv) OVER(PARTITION BY cookieid) AS pv_sum_2,
	AVG(pv) OVER(PARTITION BY cookieid ORDER BY createtime) AS pv_avg_1,
	AVG(pv) OVER(PARTITION BY cookieid) AS pv_avg_2,
	MAX(pv) OVER(PARTITION BY cookieid ORDER BY createtime) AS pv_max_1,
	MAX(pv) OVER(PARTITION BY cookieid) AS pv_max_2,
	MIN(pv) OVER(PARTITION BY cookieid ORDER BY createtime) AS pv_min_1,
	MIN(pv) OVER(PARTITION BY cookieid) AS pv_min_2,
	-- 计算数值占比
	RATIO_TO_REPORT(pv) OVER(PARTITION BY cookieid ORDER BY createtime) AS pv_ratio_to_report_1,
	RATIO_TO_REPORT(pv) OVER(PARTITION BY cookieid) AS pv_ratio_to_report_2
FROM 
	0_test
	
```

**运行结果**

cookieid | createtime | pv | pv_sum_1 | pv_sum_2 | pv_avg_1 | pv_avg_2 | pv_max_1 | pv_max_2 | pv_min_1 | pv_min_2 | pv_ratio_to_report_1 | pv_ratio_to_report_2
---|---|---|---|---|---|---|---|---|---|---|--|---|---
cookie1 | 2015-04-10 | 1 | 1 | 26 | 1.0 | 3.7142857142857144 | 1 | 7 | 1 | 1 | 1.0 | 0.038461538461538464
cookie1 | 2015-04-11 | 5 | 6 | 26 | 3.0 | 3.7142857142857144 | 5 | 7 | 1 | 1 | 0.8333333333333334 | 0.19230769230769232
cookie1 | 2015-04-12 | 7 | 13 | 26 | 4.333333333333333 | 3.7142857142857144 | 7 | 7 | 1 | 1 | 0.5384615384615384 | 0.2692307692307692
cookie1 | 2015-04-13 | 3 | 16 | 26 | 4.0 | 3.7142857142857144 | 7 | 7 | 1 | 1 | 0.1875 | 0.11538461538461539
cookie1 | 2015-04-14 | 2 | 18 | 26 | 3.6 | 3.7142857142857144 | 7 | 7 | 1 | 1 | 0.1111111111111111 | 0.07692307692307693
cookie1 | 2015-04-15 | 4 | 22 | 26 | 3.6666666666666665 | 3.7142857142857144 | 7 | 7 | 1 | 1 | 0.18181818181818182 | 0.15384615384615385
cookie1 | 2015-04-16 | 4 | 26 | 26 | 3.7142857142857144 | 3.7142857142857144 | 7 | 7 | 1 | 1 | 0.15384615384615385 | 0.15384615384615385
cookie2 | 2015-04-14 | 1 | 1 | 6 | 1.0 | 2.0 | 1 | 3 | 1 | 1 | 1.0 | 0.16666666666666666
cookie2 | 2015-04-15 | 2 | 3 | 6 | 1.5 | 2.0 | 2 | 3 | 1 | 1 | 0.6666666666666666 | 0.3333333333333333
cookie2 | 2015-04-16 | 3 | 6 | 6 | 2.0 | 2.0 | 3 | 3 | 1 | 1 | 0.5 | 0.5


```
# 为了加深分组|窗口的理解，单独介绍一下count over函数
-----------------------------------
--- COUNT( 1 | [ DISTINCT ] expr)OVER ( [query_partition_clause] [order_by_clause] )
-----------------------------------
SELECT
	cookieid,
	COUNT(1) OVER(PARTITION BY cookieid) AS count_over_1,
	COUNT(2) OVER(PARTITION BY cookieid) AS count_over_2,
	COUNT(20) OVER(PARTITION BY cookieid) AS count_over_20,
	COUNT(DISTINCT pv) OVER(PARTITION BY cookieid) AS count_over_distinct_pv
FROM
	0_test
```

**运行结果**
cookieid | count_over_1 | count_over_2 | count_over_20 | count_over_distinct_pv
---|---|---|---|---
cookie1 | 7 | 7 | 7 | 6
cookie1 | 7 | 7 | 7 | 6
cookie1 | 7 | 7 | 7 | 6
cookie1 | 7 | 7 | 7 | 6
cookie1 | 7 | 7 | 7 | 6
cookie1 | 7 | 7 | 7 | 6
cookie1 | 7 | 7 | 7 | 6
cookie2 | 3 | 3 | 3 | 3
cookie2 | 3 | 3 | 3 | 3
cookie2 | 3 | 3 | 3 | 3


```
# 类似的常规SQL
SELECT 
	cookieid,
	COUNT(1) AS count_over_1,
	COUNT(2) AS count_over_2,
	COUNT(20) AS count_over_20,
	COUNT(DISTINCT pv) AS count_over_distinct_pv
FROM 
	0_test
GROUP BY
	cookieid
```

**运行结果**
cookieid | count_over_1 | count_over_2 | count_over_20 | count_over_distinct_pv
---|---|---|---|---
cookie1 | 7 | 7 | 7 | 6
cookie2 | 3 | 3 | 3 | 3

**区别**
- 使用带有OVER的窗口函数查询出来的行数等于原始数据行数，而GROUP BY行数的分组的组数
- 窗口函数在不带ORDER BY语句的时候使用方法和效果同基本函数


# 三、排序窗口函数
ROW_NUMBER、DENSE_RANK、RANK都是进行组内排序，他们可以为每一行输出一个组内的顺序编号。区别在于
- ROW_NUMBER 每一行都有一个**唯一**的编号
- DENSE_RANK 每一行都有一个编号，数据相同的并列为一个编号，下一行数据编号连续，比如两个并列第三名，下一个就是第四名
- RANK 每一行都有一个编号，数据相同的并列为一个编号，下一行数据编号不连续，比如两个并列第三名，下一个就是第五名

常用的使用场景比如：统计每个班每个同学的成绩排名、统计每个组根据某个字段排序的第N条记录

```
-----------------------------------
-- ROW_NUMBER ( ) OVER ( [query_partition_clause] order_by_clause 
-- DENSE_RANK ( ) OVER ( [query_partition_clause] order_by_clause 
-- RANK ( ) OVER ( [query_partition_clause] order_by_clause 
-----------------------------------
SELECT 
	cookieid,
	createtime,
	pv,
	ROW_NUMBER() OVER(PARTITION BY cookieid ORDER BY pv desc) AS pv_row_number,
	DENSE_RANK() OVER(PARTITION BY cookieid ORDER BY pv desc) AS pv_dense_rank,
	RANK() OVER(PARTITION BY cookieid ORDER BY pv DESC) AS pv_rank  
FROM 0_test
```

**运行结果**

cookieid | createtime | pv | pv_row_number | pv_dense_rank | pv_rank
---|---|---|---|---|---|---|---
cookie1 | 2015-04-12 | 7 | 1 | 1 | 1
cookie1 | 2015-04-11 | 5 | 2 | 2 | 2
cookie1 | 2015-04-16 | 4 | 3 | 3 | 3
cookie1 | 2015-04-15 | 4 | 4 | 3 | 3
cookie1 | 2015-04-13 | 3 | 5 | 4 | 5
cookie1 | 2015-04-14 | 2 | 6 | 5 | 6
cookie1 | 2015-04-10 | 1 | 7 | 6 | 7
cookie2 | 2015-04-16 | 3 | 1 | 1 | 1
cookie2 | 2015-04-15 | 2 | 2 | 2 | 2
cookie2 | 2015-04-14 | 1 | 3 | 3 | 3


# 四、位移窗口函数
在每个分组中，每行记录都有自己的顺序，也就有了位移的概念。  
LAG OVER函数可以方便的获得自己前N行记录的某个字段的内容，LEAD OVER与之相反，取的是后N行的记录数据，所有offset必然都是一个正整数  
值得注意的是，每行记录在每个分组中都会有自己的顺序，统计值根据位移来统计，所以如果最后输出的顺序不一致的话可能导致每个组的统计位移和最后输出的顺序不一致的情况，建议加上ORDER BY

使用场景比如：统计每一组中最早加入的人
```
-----------------------------------
-- LAG ( value_expr [, offset ] [, default] )OVER ( [query_partition_clause] order_by_clause )
-- LEAD (value_expr [, offset ] [, default ] ) OVER ( [query_partition_clause] order_by_clause )
-- FIRST_VALUE ( expr ) OVER ( [query_partition_clause] [order_by_clause] )
-- LAST_VALUE ( expr ) OVER ( [query_partition_clause] [order_by_clause] )
-----------------------------------
SELECT 
	cookieid,
	createtime,
	pv,
	ROW_NUMBER() OVER(PARTITION BY cookieid ORDER BY createtime) AS rk,
	-- 取本组中，前两行的数据，没取到的话就填默认值, 没有默认值的话填NULL
	LAG(createtime, 2, 'default') OVER(PARTITION BY cookieid ORDER BY createtime) AS lag_createtime,
	-- LEAD完全和LAG相关，往下N行取数据
	LEAD(createtime, 2, 'default') OVER(PARTITION BY cookieid ORDER BY createtime) AS lead_createtime,
	FIRST_VALUE(createtime) OVER(PARTITION BY cookieid ORDER BY createtime) AS first_createtime, 
	LAST_VALUE(createtime) OVER(PARTITION BY cookieid ORDER BY createtime) AS last_createtime	 
FROM 
	0_test
ORDER BY
	cookieid, createtime
```

**运行结果**

cookieid | createtime | pv | rk | lag_createtime | lead_createtime | first_createtime | last_createtime |  
---|---|---|---|---|---|---|---
cookie1 | 2015-04-10 | 1 | 1 | default | 2015-04-12 | 2015-04-10 | 2015-04-16
cookie1 | 2015-04-11 | 5 | 2 | default | 2015-04-13 | 2015-04-10 | 2015-04-16
cookie1 | 2015-04-12 | 7 | 3 | 2015-04-10 | 2015-04-14 | 2015-04-10 | 2015-04-16
cookie1 | 2015-04-13 | 3 | 4 | 2015-04-11 | 2015-04-15 | 2015-04-10 | 2015-04-16
cookie1 | 2015-04-14 | 2 | 5 | 2015-04-12 | 2015-04-16 | 2015-04-10 | 2015-04-16
cookie1 | 2015-04-15 | 4 | 6 | 2015-04-13 | default | 2015-04-10 | 2015-04-16
cookie1 | 2015-04-16 | 4 | 7 | 2015-04-14 | default | 2015-04-10 | 2015-04-16
cookie2 | 2015-04-14 | 1 | 1 | default | 2015-04-16 | 2015-04-14 | 2015-04-16
cookie2 | 2015-04-15 | 2 | 2 | default | default | 2015-04-14 | 2015-04-16
cookie2 | 2015-04-16 | 3 | 3 | 2015-04-14 | default | 2015-04-14 | 2015-04-16


# 大牛文章参考
[Hive分析窗口函数(一) SUM,AVG,MIN,MAX](http://lxw1234.com/archives/2015/04/176.htm)  
[Hive分析窗口函数(二) NTILE,ROW_NUMBER,RANK,DENSE_RANK](http://lxw1234.com/archives/2015/04/181.htm)  
[Hive分析窗口函数(三) CUME_DIST,PERCENT_RANK](http://lxw1234.com/archives/2015/04/185.htm)  
[Hive分析窗口函数(四) LAG,LEAD,FIRST_VALUE,LAST_VALUE](http://lxw1234.com/archives/2015/04/190.htm)  
[Hive分析窗口函数(五) GROUPING SETS,GROUPING__ID,CUBE,ROLLUP](http://lxw1234.com/archives/2015/04/193.htm)  
[oracle 分析函数](http://blog.csdn.net/tanyit/article/details/6937366)

> @ WHAT - HOW - WHY  
> @ 不积跬步 - 无以至千里  
> @ 学必求其心得 - 业必贵其专精