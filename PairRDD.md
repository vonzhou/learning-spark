# 键值对RDD

## 创建 Pair RDD

4.2 使用第一个单词作为key创建 pair RDD

```scala
scala> var lines = sc.textFile("README.md")
lines: org.apache.spark.rdd.RDD[String] = README.md MapPartitionsRDD[1] at textFile at <console>:24

scala> var pairs = lines.map(x=>(x.split(" ")(0),x))
pairs: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[2] at map at <console>:25
```


## Pair RDD 的转换操作

4.5 过滤掉长度超过20个字符的行

```scala
scala> pairs.filter{case (key, value) => value.length < 20}
res0: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[3] at filter at <console>:26
```


4.8 使用 reduceByKey() 和 mapValues() 计算每个键对应的平均值


```scala
scala> var rdd = sc.parallelize(1 to 10)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[7] at parallelize at <console>:27

scala> var pairRdd = rdd.map( x => (x, x))
pairRdd: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[8] at map at <console>:28

scala> pairRdd.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
res2: org.apache.spark.rdd.RDD[(Int, (Int, Int))] = ShuffledRDD[10] at reduceByKey at <console>:29

scala> pairRdd.map{case (x,y) => x/y}.first
res4: Int = 1
```

4.10 实现单词计数

```scala
scala> val input = sc.textFile("README.md")
input: org.apache.spark.rdd.RDD[String] = README.md MapPartitionsRDD[13] at textFile at <console>:27

scala> val words = input.flatMap(x => x.split(" "))
words: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[14] at flatMap at <console>:28

scala> val result = words.map(x => (x, 1)).reduceByKey((x, y) => x+y)
result: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[16] at reduceByKey at <console>:28

scala> result.first
res5: (String, Int) = (package,1)
```

```scala
scala> val input = sc.textFile("README.md")
input: org.apache.spark.rdd.RDD[String] = README.md MapPartitionsRDD[18] at textFile at <console>:27

scala> input.flatMap(x => x.split(" ")).countByValue()
res7: scala.collection.Map[String,Long] = Map(site, -> 1, Please -> 4, Contributing -> 1, GraphX -> 1, project. -> 1, "" -> 72, for -> 12, find -> 1, Apache -> 1, package -> 1, Hadoop, -> 2, review -> 1, Once -> 1, For -> 3, name -> 1, this -> 1, protocols -> 1, Hive -> 2, in -> 6, "local[N]" -> 1, MASTER=spark://host:7077 -> 1, have -> 1, your -> 1, are -> 1, is -> 7, HDFS -> 1, Data. -> 1, built -> 1, thread, -> 1, examples -> 2, developing -> 1, using -> 5, system -> 1, than -> 1, Shell -> 2, mesos:// -> 1, 3"](https://cwiki.apache.org/confluence/display/MAVEN/Parallel+builds+in+Maven+3). -> 1, easiest -> 1, This -> 2, -T -> 1, [Apache -> 1, N -> 1, integration -> 1, <class> -> 1, different -> 1, "local" -> 1, README -> 1, YARN"](http://spark.apache.org/docs/latest/building-spark.htm...
scala>
```


4.13 使用 combineByKey() 求每个键对应的平均值

```scala
scala> val nums = sc.parallelize(List(Tuple2(1, 1), Tuple2(1, 3), Tuple2(2, 2), Tuple2(2, 8)))
nums: org.apache.spark.rdd.RDD[(Int, Int)] = ParallelCollectionRDD[26] at parallelize at <console>:27

scala> val results=nums.combineByKey(
     |   (v)=>(v,1),
     |   (acc:(Int,Int),v) =>(acc._1+v,acc._2+1),
     |   (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2)
     | ).map{case(key,value)=>(key,value._1/value._2.toFloat)}
results: org.apache.spark.rdd.RDD[(Int, Float)] = MapPartitionsRDD[28] at map at <console>:32

scala> results.collectAsMap().map(println)
(2,5.0)
(1,2.0)
res9: Iterable[Unit] = ArrayBuffer((), ())
```

注意理解 combineByKey 各个参数的含义。

4.16 自定义 reduceByKey() 的并行度

```scala
scala> val data = Seq(("a",3), ("b",4), ("a",1))
data: Seq[(String, Int)] = List((a,3), (b,4), (a,1))

scala> sc.parallelize(data).reduceByKey((x,y) => x+y)
res10: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[30] at reduceByKey at <console>:30

scala> sc.parallelize(data).reduceByKey((x,y) => x+y, 10)
res11: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[32] at reduceByKey at <console>:30

scala> sc.parallelize(data).reduceByKey((x,y) => x+y).collectAsMap.map(println)
(b,4)
(a,4)
res12: Iterable[Unit] = ArrayBuffer((), ())
```

4.17 内连接


```scala
scala> val userLocations = sc.parallelize(Seq((1, "shiyan"), (2, "hangzhou")))
userLocations: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[35] at parallelize at <console>:27

scala> val userNames = sc.parallelize(Seq((1, "zhangsan"), (2, "xiaoer")))
userNames: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[36] at parallelize at <console>:27

scala> val t = userLocations.join(userNames)
t: org.apache.spark.rdd.RDD[(Int, (String, String))] = MapPartitionsRDD[39] at join at <console>:30

scala> t.collectAsMap.map(println)
(2,(hangzhou,xiaoer))
(1,(shiyan,zhangsan))
res14: Iterable[Unit] = ArrayBuffer((), ())
```

4.18 左外连接、右外连接

```scala
scala> val userLocations = sc.parallelize(Seq((1, "shiyan"), (2, "hangzhou"), (15, "earth")))
userLocations: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[40] at parallelize at <console>:27

scala> val userNames = sc.parallelize(Seq((1, "zhangsan"), (2, "xiaoer"), (99, "pao")))
userNames: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[41] at parallelize at <console>:27

scala> val t = userLocations.leftOuterJoin(userNames)
t: org.apache.spark.rdd.RDD[(Int, (String, Option[String]))] = MapPartitionsRDD[44] at leftOuterJoin at <console>:30

scala> t.collectAsMap.map(println)
(2,(hangzhou,Some(xiaoer)))
(1,(shiyan,Some(zhangsan)))
(15,(earth,None))
res15: Iterable[Unit] = ArrayBuffer((), (), ())

scala> val t = userLocations.rightOuterJoin(userNames)
t: org.apache.spark.rdd.RDD[(Int, (Option[String], String))] = MapPartitionsRDD[47] at rightOuterJoin at <console>:30

scala> t.collectAsMap.map(println)
(2,(Some(hangzhou),xiaoer))
(1,(Some(shiyan),zhangsan))
(99,(None,pao))
res16: Iterable[Unit] = ArrayBuffer((), (), ())
```

排序默认是升序，也可以自定义。

```scala
scala> val input = sc.parallelize(Seq((100, "zhangsan"), (2, "xiaoer"), (99, "pao")))
input: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[48] at parallelize at <console>:27

scala> val t = input.sortByKey()
t: org.apache.spark.rdd.RDD[(Int, String)] = ShuffledRDD[54] at sortByKey at <console>:28

scala> t.collect.map(println)
(2,xiaoer)
(99,pao)
(100,zhangsan)
res19: Array[Unit] = Array((), (), ())
```
4.20 以字符串顺序对整数进行自定义排序

```scala
scala> implicit val sortIntByString = new Ordering[Int] {
     | override def compare(a:Int, b:Int) = a.toString.compare(b.toString)
     | }
sortIntByString: Ordering[Int] = $anon$1@ae19056

scala> val t = input.sortByKey()
t: org.apache.spark.rdd.RDD[(Int, String)] = ShuffledRDD[57] at sortByKey at <console>:30

scala> t.collect.map(println)
(100,zhangsan)
(2,xiaoer)
(99,pao)
res20: Array[Unit] = Array((), (), ())
```


## Pair RDD 的Action操作

## 数据分区
