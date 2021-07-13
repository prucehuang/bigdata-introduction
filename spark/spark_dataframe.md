[toc]

记录一下 日常使用的df增删改查API，持续更新...
[官网API传送门](https://spark.apache.org/docs/latest/api/python/reference/index.html)


# 一、创建

```python
# 从pandas df来
color_df=spark.createDataFrame(color_df)
color_df.show()

# 自我构造
from pyspark.sql import Row
row = Row("spe_id", "InOther")
x = ['x1','x2']
y = ['y1','y2']
new_df = sc.parallelize([row(x[i], y[i]) for i in range(2)]).toDF()
```

# 二、查询

## 2.1 基本信息

### show、head、collect
``` python
# 查看数据case
df.show()
df.show(30)
df.head(3).show()
df.take(5).show()

# 选择列数据
df.age.show()
df["age"].show()

df.collect()

# 打印全部信息，不加省略号
.show(20, False)
```

### select
``` python
df.select(“name”)
df.select(df[‘name’], df[‘age’]+1)
df.select(df.a, df.b, df.c)
df.select(df["a"], df["b"], df["c"])
data.select('columns').distinct().show()
```

### where
``` python
df.where("id = 1 or c1 = 'b'" ).show()
df.where("color like '%yellow%'")
```

### schema
```python
df.printSchema()
df.dtypes
df.columns
```

### filter
```python
from pyspark.sql.functions import isnull
df = df.filter(isnull("col_a"))
df.filter("color like 'b%'").show()

df = df.filter(df['age']>21)
df = df.where(df['age']>21)
```

### join

```
df = df1.join(df2, key)\
        .select(df1.vroleid, df1.label1, df1.label2, df1.cprediction, df2.rprediction)
```

### case when

```python
from pyspark.sql import functions as F
df.select(df.name, F.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)).show()

# 或者
df = df.withColumn('prediction', when(df.cprediction==0, 10).when(df.cprediction>0, df2.rprediction).otherwise(0))
df.show(10)
```

### between

```python
>> df.select(df.name, df.age.between(2, 4)).show()
+-----+---------------------------+
| name|((age >= 2) AND (age <= 4))|
+-----+---------------------------+
|Alice|                       true|
|  Bob|                      false|
+-----+---------------------------+
```

### group by

```python
# 方式一，聚合函数更简洁
df[['izoneareaid', 'vroleid', 'huodongid', 'goumaijine']] \
    .groupBy(['izoneareaid', 'huodongid']) \
    .agg({'vroleid':'count', 'goumaijine':'sum'}) \
    .withColumnRenamed('sum(goumaijine)', 'goumaijine_sum')
    .sort('goumaijine_sum', ascending=False) \
    .show()
    
# 方式二，重命名更简洁
from pyspark.sql import functions as f
df[['izoneareaid', 'vroleid', 'huodongid', 'goumaijine']] \
    .groupBy(['izoneareaid', 'huodongid']) \
    .agg(f.count(huodong_df.vroleid),
         f.sum(huodong_df.goumaijine).alias('goumaijine_sum')) \
    .sort('goumaijine_sum', ascending=False) \
    .show()
```

## 2.2 统计信息

```python
df.count()
df.describe().show()
```

## 2.3 排序

```python
# spark排序
color_df.sort('color', ascending=False).show()

# 多字段排序
color_df.filter(color_df['length']>=4)\
        .sort('length', 'color', ascending=False).show()

# 混合排序
color_df.sort(color_df.length.desc(), color_df.color.asc()).show()

# orderBy也是排序，返回的Row对象列表
color_df.orderBy('length','color').take(4)
train.orderBy(train.Purchase.desc()).show(5)
```

## 2.4 抽样

```python
# DataFrame.sample(withReplacement=None, fraction=None, seed=None)
t1 = train.sample(False, 0.2, 42)
t2 = train.sample(False, 0.2, 43)
```

## 2.6 直接使用SQL语法

```python
# 首先dataframe注册为临时表，然后执行SQL查询
color_df.createOrReplaceTempView("color_df")
spark.sql("select count(1) from color_df").show()
```

# 三、修改

## 3.1 修改列名
```python
# spark-1
# 在创建dataframe的时候重命名
data = spark.createDataFrame(data=[("Alberto", 2), ("Dakota", 2)],
                              schema=['name','length'])
data.show()
data.printSchema()

# spark-2
# 使用selectExpr方法
color_df2 = color_df.selectExpr('color as color2','length as length2')
color_df2.show()

# spark-3
# withColumnRenamed方法
color_df2 = color_df.withColumnRenamed('color','color2')\
                    .withColumnRenamed('length','length2')
color_df2.show()

# spark-4
# alias 方法
color_df.select(color_df.color.alias('color2')).show()
```

## 3.2 新增一列
```python
train.withColumn('Purchase_new', train.Purchase /2.0).select('Purchase','Purchase_new').show(5)
Output:
+--------+------------+
|Purchase|Purchase_new|
+--------+------------+
|    8370|      4185.0|
|   15200|      7600.0|
|    1422|       711.0|
|    1057|       528.5|
|    7969|      3984.5|
+--------+------------+

# 新增一列常数项
huodong_df = huodong_df.withColumnRenamed('vopenid', 'vopenid_hd')\
                       .withColumn(huodong_label, F.lit(111))
    
# 保留有效数字
df.withColumn('prediction_error(%)', F.bround( \
              100*(df['sum(prediction)']-df['sum(label2)']) / df['sum(label2)'], \
              scale=4))  \
         .sort('sum(label2)', ascending=False) \
         .show(20, False)
```

## 3.2 修改列数据类型
```python
df = df.withColumn('vkaifushijian', df.vkaifushijian.cast('int'))\
	   .withColumn('vrolesex', df.vrolesex.cast('int'))
```

## 3.3 Pyspark DF 和 Pandas DF 互换

```python
# from pyspark.sql.dataframe.DataFrame to pandas.core.frame.DataFrame
df_p = df_s.toPandas()
# 换回来
df_s = spark.createDataFrame(df_p)
```

# 四、删除

```python
# 删除一列
color_df.drop('length').show()

# drop na
df2 = spark_df.dropna()
df2.show()

# 或者
spark_df=spark_df.na.drop()

# 删除变量
del df
```

# 五、保存

``` python
df.to_csv("stock.csv")
```



> 参考
[PySpark-DataFrame各种常用操作举例](https://blog.csdn.net/anshuai_aw1/article/details/87881079)  
[PySpark︱DataFrame操作指南：增/删/改/查/合并/统计与数据处理](https://blog.csdn.net/sinat_26917383/article/details/80500349)