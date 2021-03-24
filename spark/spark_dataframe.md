[toc]

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

### 打印schema
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

## join

```
train_predictions = gbtc_train_predictions.join(gbtr_train_predictions, key)\
                                          .select(gbtc_train_predictions.vroleid, gbtc_train_predictions.label1, gbtc_train_predictions.label2, 
                                                  gbtc_train_predictions.cprediction, gbtr_train_predictions.rprediction)

train_predictions = train_predictions.withColumn('prediction', when(train_predictions.cprediction==0, 0)\
                                                 .when(train_predictions.cprediction>0, gbtr_train_predictions.rprediction)\
                                                 .otherwise(0))
train_predictions.show(10)
```







## 2.2 统计信息

```python
df.count()
df.describe().show()
```



## 2.3 排序

```python
# spark排序
color_df.sort('color',ascending=False).show()

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
t1 = train.sample(False, 0.2, 42)
t2 = train.sample(False, 0.2, 43)
```



## 2.5 case when

```python
from pyspark.sql import functions as F
df.select(df.name, F.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)).show()
```

```python
>> df.select(df.name, df.age.between(2, 4)).show()
+-----+---------------------------+
| name|((age >= 2) AND (age <= 4))|
+-----+---------------------------+
|Alice|                       true|
|  Bob|                      false|
+-----+---------------------------+
```

## 2.6 直接使用SQL语法

```python
# 首先dataframe注册为临时表，然后执行SQL查询
color_df.createOrReplaceTempView("color_df")
spark.sql("select count(1) from color_df").show()
```



## 2.7 Group By

```python
# 方式一，聚合函数更简洁
huodong_df[['izoneareaid', 'vroleid', 'huodongid', 'goumaijine']] \
    .groupBy(['izoneareaid', 'huodongid']) \
    .agg({'vroleid':'count', 'goumaijine':'sum'}) \
    .withColumnRenamed('sum(goumaijine)', 'goumaijine_sum')
    .sort('goumaijine_sum', ascending=False) \
    .show()
    
# 方式二，重命名更简洁
from pyspark.sql import functions as f
huodong_df[['izoneareaid', 'vroleid', 'huodongid', 'goumaijine']] \
    .groupBy(['izoneareaid', 'huodongid']) \
    .agg(f.count(huodong_df.vroleid),
         f.sum(huodong_df.goumaijine).alias('goumaijine_sum')) \
    .sort('goumaijine_sum', ascending=False) \
    .show()
```



# 三、修改



## 3.1 增加列名

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

df = df.withColumn("year2", df["year1"].cast("Int"))

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



## 3.2 修改列数据类型

```python
train_df_s = train_df_s.withColumn('vkaifushijian', train_df_s.vkaifushijian.cast('int'))\
            					 .withColumn('vrolesex', train_df_s.vrolesex.cast('int'))\
            					 .withColumn('vroleprofession', train_df_s.vroleprofession.cast('int'))\
            					 .withColumnRenamed(target, 'label')
```



## 3.3 pyspark dataframe 和 pandas DataFrame 互换

```python
# from pyspark.sql.dataframe.DataFrame to pandas.core.frame.DataFrame
train_df = train_df_s.toPandas()
# 换回来
test_df_s = spark.createDataFrame(test_X_p)
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
```



# 五、保存

``` python
df.to_csv("stock.csv")
```



> 参考
[PySpark-DataFrame各种常用操作举例](https://blog.csdn.net/anshuai_aw1/article/details/87881079)  
[PySpark︱DataFrame操作指南：增/删/改/查/合并/统计与数据处理](https://blog.csdn.net/sinat_26917383/article/details/80500349)