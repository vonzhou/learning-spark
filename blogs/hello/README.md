[主页](https://github.com/vonzhou/Blog)  | [读书](https://github.com/vonzhou/readings)  | [知乎](https://www.zhihu.com/people/vonzhou)
---
# Spark 快速入门


## 安装

* java
* spark-2.4.0-bin-hadoop2.7
* 本地模式


## 初识

```
vonzhou@ubuntu:~/spark-2.4.0-bin-hadoop2.7$ ./bin/spark-shell
19/03/07 06:34:55 WARN Utils: Your hostname, ubuntu resolves to a loopback address: 127.0.1.1; using 10.240.208.36 instead (on interface eth0)
19/03/07 06:34:55 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
19/03/07 06:34:55 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark context Web UI available at http://10.240.208.36:4040
Spark context available as 'sc' (master = local[*], app id = local-1551940503852).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.0
      /_/

Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_191)
Type in expressions to have them evaluated.
Type :help for more information.

scala> val lines = sc.textFile("README.md")
lines: org.apache.spark.rdd.RDD[String] = README.md MapPartitionsRDD[1] at textFile at <console>:24

scala> lines.count()
res0: Long = 105

scala> lines.first()
res1: String = # Apache Spark
```

通过 spark UI http://10.240.208.36:4040 可以看到Job的执行情况。


SparkContext类型的变量sc抽象的是对计算集群的一个连接。

```
scala> sc
res2: org.apache.spark.SparkContext = org.apache.spark.SparkContext@423ddfe1
```


筛选特定单词的行：

```
scala> val lines = sc.textFile("README.md")
lines: org.apache.spark.rdd.RDD[String] = README.md MapPartitionsRDD[3] at textFile at <console>:24

scala> val pythonLines = lines.filter(line => line.contains("Python"))
pythonLines: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[4] at filter at <console>:25

scala> pythonLines.first()
res3: String = high-level APIs in Scala, Java, Python, and R, and an optimized engine that
```


##  独立应用 WordCount

```scala
import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input = sc.textFile(inputFile)
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile)
  }
}
```



构建：

```
vonzhou@ubuntu:~/learning-spark/mini-complete-example$ sbt clean package
[info] Updated file /home/vonzhou/learning-spark/mini-complete-example/project/build.properties: set sbt.version to 1.2.6
[info] Loading settings for project mini-complete-example-build from plugins.sbt ...
[info] Loading project definition from /home/vonzhou/learning-spark/mini-complete-example/project
[info] Updating ProjectRef(uri("file:/home/vonzhou/learning-spark/mini-complete-example/project/"), "mini-complete-example-build")...
[info] Done updating.
[info] Loading settings for project mini-complete-example from build.sbt ...
[info] Set current project to learning-spark-mini-example (in build file:/home/vonzhou/learning-spark/mini-complete-example/)
[success] Total time: 0 s, completed Mar 7, 2019 7:02:54 AM
[info] Updating ...
.....
[info] Packaging /home/vonzhou/learning-spark/mini-complete-example/target/scala-2.11/learning-spark-mini-example_2.11-0.0.1.jar ...
[info] Done packaging.
[success] Total time: 285 s, completed Mar 7, 2019 7:07:39 AM
```

然后使用 spark-submmit 提交到集群进行执行。


```
vonzhou@ubuntu:~/spark-2.4.0-bin-hadoop2.7$ ./bin/spark-submit --class com.oreilly.learningsparkexamples.mini.scala.WordCount  ../learning-spark/mini-complete-example/target/scala-2.11/learning-spark-mini-example_2.11-0.0.1.jar  ./README.md ./wordcounts
19/03/07 07:13:24 WARN Utils: Your hostname, ubuntu resolves to a loopback address: 127.0.1.1; using 10.240.208.36 instead (on interface eth0)
19/03/07 07:13:24 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
19/03/07 07:13:24 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
19/03/07 07:13:24 INFO SparkContext: Running Spark version 2.4.0
19/03/07 07:13:24 INFO SparkContext: Submitted application: wordCount
19/03/07 07:13:24 INFO SecurityManager: Changing view acls to: vonzhou
19/03/07 07:13:24 INFO SecurityManager: Changing modify acls to: vonzhou
19/03/07 07:13:24 INFO SecurityManager: Changing view acls groups to:
19/03/07 07:13:24 INFO SecurityManager: Changing modify acls groups to:
19/03/07 07:13:24 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(vonzhou); groups with view permissions: Set(); users  with modify permissions: Set(vonzhou); groups with modify permissions: Set()
19/03/07 07:13:25 DEBUG InternalLoggerFactory: Using SLF4J as the default logging framework
19/03/07 07:13:25 DEBUG InternalThreadLocalMap: -Dio.netty.threadLocalMap.stringBuilder.initialSize: 1024
19/03/07 07:13:25 DEBUG InternalThreadLocalMap: -Dio.netty.threadLocalMap.stringBuilder.maxSize: 4096
19/03/07 07:13:25 DEBUG MultithreadEventLoopGroup: -Dio.netty.eventLoopThreads: 16
19/03/07 07:13:25 DEBUG PlatformDependent0: -Dio.netty.noUnsafe: false
19/03/07 07:13:25 DEBUG PlatformDependent0: Java version: 8
19/03/07 07:13:25 DEBUG PlatformDependent0: sun.misc.Unsafe.theUnsafe: available
19/03/07 07:13:25 DEBUG PlatformDependent0: sun.misc.Unsafe.copyMemory: available
19/03/07 07:13:25 DEBUG PlatformDependent0: java.nio.Buffer.address: available
19/03/07 07:13:25 DEBUG PlatformDependent0: direct buffer constructor: available
19/03/07 07:13:25 DEBUG PlatformDependent0: java.nio.Bits.unaligned: available, true
19/03/07 07:13:25 DEBUG PlatformDependent0: jdk.internal.misc.Unsafe.allocateUninitializedArray(int): unavailable prior to Java9
19/03/07 07:13:25 DEBUG PlatformDependent0: java.nio.DirectByteBuffer.<init>(long, int): available
19/03/07 07:13:25 DEBUG PlatformDependent: sun.misc.Unsafe: available
19/03/07 07:13:25 DEBUG PlatformDependent: -Dio.netty.tmpdir: /tmp (java.io.tmpdir)
19/03/07 07:13:25 DEBUG PlatformDependent: -Dio.netty.bitMode: 64 (sun.arch.data.model)
19/03/07 07:13:25 DEBUG PlatformDependent: -Dio.netty.noPreferDirect: false
19/03/07 07:13:25 DEBUG PlatformDependent: -Dio.netty.maxDirectMemory: 954728448 bytes
19/03/07 07:13:25 DEBUG PlatformDependent: -Dio.netty.uninitializedArrayAllocationThreshold: -1
19/03/07 07:13:25 DEBUG CleanerJava6: java.nio.ByteBuffer.cleaner(): available
19/03/07 07:13:25 DEBUG NioEventLoop: -Dio.netty.noKeySetOptimization: false
19/03/07 07:13:25 DEBUG NioEventLoop: -Dio.netty.selectorAutoRebuildThreshold: 512
19/03/07 07:13:25 DEBUG PlatformDependent: org.jctools-core.MpscChunkedArrayQueue: available
19/03/07 07:13:25 DEBUG ResourceLeakDetector: -Dio.netty.leakDetection.level: simple
19/03/07 07:13:25 DEBUG ResourceLeakDetector: -Dio.netty.leakDetection.targetRecords: 4
19/03/07 07:13:25 DEBUG PooledByteBufAllocator: -Dio.netty.allocator.numHeapArenas: 9
19/03/07 07:13:25 DEBUG PooledByteBufAllocator: -Dio.netty.allocator.numDirectArenas: 9
19/03/07 07:13:25 DEBUG PooledByteBufAllocator: -Dio.netty.allocator.pageSize: 8192
19/03/07 07:13:25 DEBUG PooledByteBufAllocator: -Dio.netty.allocator.maxOrder: 11
19/03/07 07:13:25 DEBUG PooledByteBufAllocator: -Dio.netty.allocator.chunkSize: 16777216
19/03/07 07:13:25 DEBUG PooledByteBufAllocator: -Dio.netty.allocator.tinyCacheSize: 512
19/03/07 07:13:25 DEBUG PooledByteBufAllocator: -Dio.netty.allocator.smallCacheSize: 256
19/03/07 07:13:25 DEBUG PooledByteBufAllocator: -Dio.netty.allocator.normalCacheSize: 64
19/03/07 07:13:25 DEBUG PooledByteBufAllocator: -Dio.netty.allocator.maxCachedBufferCapacity: 32768
19/03/07 07:13:25 DEBUG PooledByteBufAllocator: -Dio.netty.allocator.cacheTrimInterval: 8192
19/03/07 07:13:25 DEBUG PooledByteBufAllocator: -Dio.netty.allocator.useCacheForAllThreads: true
19/03/07 07:13:25 DEBUG DefaultChannelId: -Dio.netty.processId: 8101 (auto-detected)
19/03/07 07:13:25 DEBUG NetUtil: -Djava.net.preferIPv4Stack: false
19/03/07 07:13:25 DEBUG NetUtil: -Djava.net.preferIPv6Addresses: false
19/03/07 07:13:25 DEBUG NetUtil: Loopback interface: lo (lo, 0:0:0:0:0:0:0:1%lo)
19/03/07 07:13:25 DEBUG NetUtil: /proc/sys/net/core/somaxconn: 128
19/03/07 07:13:25 DEBUG DefaultChannelId: -Dio.netty.machineId: d8:9c:67:ff:fe:57:f4:17 (auto-detected)
19/03/07 07:13:25 DEBUG ByteBufUtil: -Dio.netty.allocator.type: pooled
19/03/07 07:13:25 DEBUG ByteBufUtil: -Dio.netty.threadLocalDirectBufferSize: 65536
19/03/07 07:13:25 DEBUG ByteBufUtil: -Dio.netty.maxThreadLocalCharBufferSize: 16384
19/03/07 07:13:25 DEBUG TransportServer: Shuffle server started on port: 53246
19/03/07 07:13:25 INFO Utils: Successfully started service 'sparkDriver' on port 53246.
19/03/07 07:13:25 DEBUG SparkEnv: Using serializer: class org.apache.spark.serializer.JavaSerializer
19/03/07 07:13:25 INFO SparkEnv: Registering MapOutputTracker
19/03/07 07:13:25 DEBUG MapOutputTrackerMasterEndpoint: init
19/03/07 07:13:25 INFO SparkEnv: Registering BlockManagerMaster
19/03/07 07:13:25 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
19/03/07 07:13:25 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
19/03/07 07:13:25 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-22dd20e2-1053-4348-a5cb-69cf11012e0e
19/03/07 07:13:25 DEBUG DiskBlockManager: Adding shutdown hook
19/03/07 07:13:25 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
19/03/07 07:13:25 INFO SparkEnv: Registering OutputCommitCoordinator
19/03/07 07:13:25 DEBUG OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: init
19/03/07 07:13:25 DEBUG SecurityManager: Created SSL options for ui: SSLOptions{enabled=false, port=None, keyStore=None, keyStorePassword=None, trustStore=None, trustStorePassword=None, protocol=None, enabledAlgorithms=Set()}
19/03/07 07:13:26 INFO Utils: Successfully started service 'SparkUI' on port 4040.
19/03/07 07:13:26 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://10.240.208.36:4040
19/03/07 07:13:26 INFO SparkContext: Added JAR file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/../learning-spark/mini-complete-example/target/scala-2.11/learning-spark-mini-example_2.11-0.0.1.jar at spark://10.240.208.36:53246/jars/learning-spark-mini-example_2.11-0.0.1.jar with timestamp 1551942806186
19/03/07 07:13:26 INFO Executor: Starting executor ID driver on host localhost
19/03/07 07:13:26 DEBUG TransportServer: Shuffle server started on port: 53247
19/03/07 07:13:26 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 53247.
19/03/07 07:13:26 INFO NettyBlockTransferService: Server created on 10.240.208.36:53247
19/03/07 07:13:26 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
19/03/07 07:13:26 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 10.240.208.36, 53247, None)
19/03/07 07:13:26 DEBUG DefaultTopologyMapper: Got a request for 10.240.208.36
19/03/07 07:13:26 INFO BlockManagerMasterEndpoint: Registering block manager 10.240.208.36:53247 with 366.3 MB RAM, BlockManagerId(driver, 10.240.208.36, 53247, None)
19/03/07 07:13:26 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 10.240.208.36, 53247, None)
19/03/07 07:13:26 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 10.240.208.36, 53247, None)
19/03/07 07:13:26 DEBUG SparkContext: Adding shutdown hook
19/03/07 07:13:27 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 236.7 KB, free 366.1 MB)
19/03/07 07:13:27 DEBUG BlockManager: Put block broadcast_0 locally took  120 ms
19/03/07 07:13:27 DEBUG BlockManager: Putting block broadcast_0 without replication took  123 ms
19/03/07 07:13:27 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 22.9 KB, free 366.0 MB)
19/03/07 07:13:27 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 10.240.208.36:53247 (size: 22.9 KB, free: 366.3 MB)
19/03/07 07:13:27 DEBUG BlockManagerMaster: Updated info of block broadcast_0_piece0
19/03/07 07:13:27 DEBUG BlockManager: Told master about block broadcast_0_piece0
19/03/07 07:13:27 DEBUG BlockManager: Put block broadcast_0_piece0 locally took  9 ms
19/03/07 07:13:27 DEBUG BlockManager: Putting block broadcast_0_piece0 without replication took  9 ms
19/03/07 07:13:27 INFO SparkContext: Created broadcast 0 from textFile at WordCount.scala:17
19/03/07 07:13:27 DEBUG ClosureCleaner: +++ Cleaning closure <function1> (org.apache.spark.SparkContext$$anonfun$hadoopFile$1$$anonfun$30) +++
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared fields: 2
19/03/07 07:13:27 DEBUG ClosureCleaner:      public static final long org.apache.spark.SparkContext$$anonfun$hadoopFile$1$$anonfun$30.serialVersionUID
19/03/07 07:13:27 DEBUG ClosureCleaner:      private final org.apache.spark.SparkContext$$anonfun$hadoopFile$1 org.apache.spark.SparkContext$$anonfun$hadoopFile$1$$anonfun$30.$outer
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared methods: 2
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final java.lang.Object org.apache.spark.SparkContext$$anonfun$hadoopFile$1$$anonfun$30.apply(java.lang.Object)
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final void org.apache.spark.SparkContext$$anonfun$hadoopFile$1$$anonfun$30.apply(org.apache.hadoop.mapred.JobConf)
19/03/07 07:13:27 DEBUG ClosureCleaner:  + inner classes: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer classes: 2
19/03/07 07:13:27 DEBUG ClosureCleaner:      org.apache.spark.SparkContext$$anonfun$hadoopFile$1
19/03/07 07:13:27 DEBUG ClosureCleaner:      org.apache.spark.SparkContext
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer objects: 2
19/03/07 07:13:27 DEBUG ClosureCleaner:      <function0>
19/03/07 07:13:27 DEBUG ClosureCleaner:      org.apache.spark.SparkContext@fe34b86
19/03/07 07:13:27 DEBUG ClosureCleaner:  + populating accessed fields because this is the starting closure
19/03/07 07:13:27 DEBUG ClosureCleaner:  + fields accessed by starting closure: 4
19/03/07 07:13:27 DEBUG ClosureCleaner:      (class java.lang.Object,Set())
19/03/07 07:13:27 DEBUG ClosureCleaner:      (class org.apache.spark.SparkContext$$anonfun$hadoopFile$1,Set(path$6))
19/03/07 07:13:27 DEBUG ClosureCleaner:      (class org.apache.spark.SparkContext,Set())
19/03/07 07:13:27 DEBUG ClosureCleaner:      (class scala.runtime.AbstractFunction0,Set())
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outermost object is not a closure or REPL line object,so do not clone it: (class org.apache.spark.SparkContext,org.apache.spark.SparkContext@fe34b86)
19/03/07 07:13:27 DEBUG ClosureCleaner:  + cloning the object <function0> of class org.apache.spark.SparkContext$$anonfun$hadoopFile$1
19/03/07 07:13:27 DEBUG ClosureCleaner:  + cleaning cloned closure <function0> recursively (org.apache.spark.SparkContext$$anonfun$hadoopFile$1)
19/03/07 07:13:27 DEBUG ClosureCleaner: +++ Cleaning closure <function0> (org.apache.spark.SparkContext$$anonfun$hadoopFile$1) +++
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared fields: 7
19/03/07 07:13:27 DEBUG ClosureCleaner:      public static final long org.apache.spark.SparkContext$$anonfun$hadoopFile$1.serialVersionUID
19/03/07 07:13:27 DEBUG ClosureCleaner:      private final org.apache.spark.SparkContext org.apache.spark.SparkContext$$anonfun$hadoopFile$1.$outer
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final java.lang.String org.apache.spark.SparkContext$$anonfun$hadoopFile$1.path$6
19/03/07 07:13:27 DEBUG ClosureCleaner:      private final java.lang.Class org.apache.spark.SparkContext$$anonfun$hadoopFile$1.inputFormatClass$1
19/03/07 07:13:27 DEBUG ClosureCleaner:      private final java.lang.Class org.apache.spark.SparkContext$$anonfun$hadoopFile$1.keyClass$1
19/03/07 07:13:27 DEBUG ClosureCleaner:      private final java.lang.Class org.apache.spark.SparkContext$$anonfun$hadoopFile$1.valueClass$1
19/03/07 07:13:27 DEBUG ClosureCleaner:      private final int org.apache.spark.SparkContext$$anonfun$hadoopFile$1.minPartitions$3
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared methods: 2
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final java.lang.Object org.apache.spark.SparkContext$$anonfun$hadoopFile$1.apply()
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final org.apache.spark.rdd.HadoopRDD org.apache.spark.SparkContext$$anonfun$hadoopFile$1.apply()
19/03/07 07:13:27 DEBUG ClosureCleaner:  + inner classes: 1
19/03/07 07:13:27 DEBUG ClosureCleaner:      org.apache.spark.SparkContext$$anonfun$hadoopFile$1$$anonfun$30
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer classes: 1
19/03/07 07:13:27 DEBUG ClosureCleaner:      org.apache.spark.SparkContext
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer objects: 1
19/03/07 07:13:27 DEBUG ClosureCleaner:      org.apache.spark.SparkContext@fe34b86
19/03/07 07:13:27 DEBUG ClosureCleaner:  + fields accessed by starting closure: 4
19/03/07 07:13:27 DEBUG ClosureCleaner:      (class java.lang.Object,Set())
19/03/07 07:13:27 DEBUG ClosureCleaner:      (class org.apache.spark.SparkContext$$anonfun$hadoopFile$1,Set(path$6))
19/03/07 07:13:27 DEBUG ClosureCleaner:      (class org.apache.spark.SparkContext,Set())
19/03/07 07:13:27 DEBUG ClosureCleaner:      (class scala.runtime.AbstractFunction0,Set())
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outermost object is not a closure or REPL line object,so do not clone it: (class org.apache.spark.SparkContext,org.apache.spark.SparkContext@fe34b86)
19/03/07 07:13:27 DEBUG ClosureCleaner:  + the starting closure doesn't actually need org.apache.spark.SparkContext@fe34b86, so we null it out
19/03/07 07:13:27 DEBUG ClosureCleaner:  +++ closure <function0> (org.apache.spark.SparkContext$$anonfun$hadoopFile$1) is now cleaned +++
19/03/07 07:13:27 DEBUG ClosureCleaner:  +++ closure <function1> (org.apache.spark.SparkContext$$anonfun$hadoopFile$1$$anonfun$30) is now cleaned +++
19/03/07 07:13:27 DEBUG ClosureCleaner: +++ Cleaning closure <function1> (org.apache.spark.SparkContext$$anonfun$textFile$1$$anonfun$apply$11) +++
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared fields: 1
19/03/07 07:13:27 DEBUG ClosureCleaner:      public static final long org.apache.spark.SparkContext$$anonfun$textFile$1$$anonfun$apply$11.serialVersionUID
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared methods: 2
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final java.lang.Object org.apache.spark.SparkContext$$anonfun$textFile$1$$anonfun$apply$11.apply(java.lang.Object)
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final java.lang.String org.apache.spark.SparkContext$$anonfun$textFile$1$$anonfun$apply$11.apply(scala.Tuple2)
19/03/07 07:13:27 DEBUG ClosureCleaner:  + inner classes: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer classes: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer objects: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + populating accessed fields because this is the starting closure
19/03/07 07:13:27 DEBUG ClosureCleaner:  + fields accessed by starting closure: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + there are no enclosing objects!
19/03/07 07:13:27 DEBUG ClosureCleaner:  +++ closure <function1> (org.apache.spark.SparkContext$$anonfun$textFile$1$$anonfun$apply$11) is now cleaned +++
19/03/07 07:13:27 DEBUG ClosureCleaner: +++ Cleaning closure <function1> (com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$2) +++
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared fields: 1
19/03/07 07:13:27 DEBUG ClosureCleaner:      public static final long com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$2.serialVersionUID
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared methods: 2
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final java.lang.Object com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$2.apply(java.lang.Object)
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final scala.collection.mutable.ArrayOps com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$2.apply(java.lang.String)
19/03/07 07:13:27 DEBUG ClosureCleaner:  + inner classes: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer classes: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer objects: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + populating accessed fields because this is the starting closure
19/03/07 07:13:27 DEBUG ClosureCleaner:  + fields accessed by starting closure: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + there are no enclosing objects!
19/03/07 07:13:27 DEBUG ClosureCleaner:  +++ closure <function1> (com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$2) is now cleaned +++
19/03/07 07:13:27 DEBUG ClosureCleaner: +++ Cleaning closure <function1> (com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$3) +++
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared fields: 1
19/03/07 07:13:27 DEBUG ClosureCleaner:      public static final long com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$3.serialVersionUID
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared methods: 2
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final java.lang.Object com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$3.apply(java.lang.Object)
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final scala.Tuple2 com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$3.apply(java.lang.String)
19/03/07 07:13:27 DEBUG ClosureCleaner:  + inner classes: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer classes: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer objects: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + populating accessed fields because this is the starting closure
19/03/07 07:13:27 DEBUG ClosureCleaner:  + fields accessed by starting closure: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + there are no enclosing objects!
19/03/07 07:13:27 DEBUG ClosureCleaner:  +++ closure <function1> (com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$3) is now cleaned +++
19/03/07 07:13:27 DEBUG BlockManager: Getting local block broadcast_0
19/03/07 07:13:27 DEBUG BlockManager: Level for block broadcast_0 is StorageLevel(disk, memory, deserialized, 1 replicas)
19/03/07 07:13:27 DEBUG HadoopRDD: Creating new JobConf and caching it for later re-use
19/03/07 07:13:27 DEBUG FileInputFormat: Time taken to get FileStatuses: 4
19/03/07 07:13:27 INFO FileInputFormat: Total input paths to process : 1
19/03/07 07:13:27 DEBUG FileInputFormat: Total # of splits generated by getSplits: 2, TimeTaken: 15
19/03/07 07:13:27 DEBUG ClosureCleaner: +++ Cleaning closure <function1> (org.apache.spark.rdd.PairRDDFunctions$$anonfun$reduceByKey$1$$anonfun$apply$10) +++
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared fields: 1
19/03/07 07:13:27 DEBUG ClosureCleaner:      public static final long org.apache.spark.rdd.PairRDDFunctions$$anonfun$reduceByKey$1$$anonfun$apply$10.serialVersionUID
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared methods: 1
19/03/07 07:13:27 DEBUG ClosureCleaner:      public final java.lang.Object org.apache.spark.rdd.PairRDDFunctions$$anonfun$reduceByKey$1$$anonfun$apply$10.apply(java.lang.Object)
19/03/07 07:13:27 DEBUG ClosureCleaner:  + inner classes: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer classes: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + outer objects: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + populating accessed fields because this is the starting closure
19/03/07 07:13:27 DEBUG ClosureCleaner:  + fields accessed by starting closure: 0
19/03/07 07:13:27 DEBUG ClosureCleaner:  + there are no enclosing objects!
19/03/07 07:13:27 DEBUG ClosureCleaner:  +++ closure <function1> (org.apache.spark.rdd.PairRDDFunctions$$anonfun$reduceByKey$1$$anonfun$apply$10) is now cleaned +++
19/03/07 07:13:27 DEBUG ClosureCleaner: +++ Cleaning closure <function2> (com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1) +++
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared fields: 1
19/03/07 07:13:27 DEBUG ClosureCleaner:      public static final long com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1.serialVersionUID
19/03/07 07:13:27 DEBUG ClosureCleaner:  + declared methods: 3
19/03/07 07:13:28 DEBUG ClosureCleaner:      public int com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1.apply$mcIII$sp(int,int)
19/03/07 07:13:28 DEBUG ClosureCleaner:      public final int com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1.apply(int,int)
19/03/07 07:13:28 DEBUG ClosureCleaner:      public final java.lang.Object com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1.apply(java.lang.Object,java.lang.Object)
19/03/07 07:13:28 DEBUG ClosureCleaner:  + inner classes: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + outer classes: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + outer objects: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + populating accessed fields because this is the starting closure
19/03/07 07:13:28 DEBUG ClosureCleaner:  + fields accessed by starting closure: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + there are no enclosing objects!
19/03/07 07:13:28 DEBUG ClosureCleaner:  +++ closure <function2> (com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1) is now cleaned +++
19/03/07 07:13:28 DEBUG ClosureCleaner: +++ Cleaning closure <function2> (com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1) +++
19/03/07 07:13:28 DEBUG ClosureCleaner:  + declared fields: 1
19/03/07 07:13:28 DEBUG ClosureCleaner:      public static final long com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1.serialVersionUID
19/03/07 07:13:28 DEBUG ClosureCleaner:  + declared methods: 3
19/03/07 07:13:28 DEBUG ClosureCleaner:      public int com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1.apply$mcIII$sp(int,int)
19/03/07 07:13:28 DEBUG ClosureCleaner:      public final int com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1.apply(int,int)
19/03/07 07:13:28 DEBUG ClosureCleaner:      public final java.lang.Object com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1.apply(java.lang.Object,java.lang.Object)
19/03/07 07:13:28 DEBUG ClosureCleaner:  + inner classes: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + outer classes: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + outer objects: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + populating accessed fields because this is the starting closure
19/03/07 07:13:28 DEBUG ClosureCleaner:  + fields accessed by starting closure: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + there are no enclosing objects!
19/03/07 07:13:28 DEBUG ClosureCleaner:  +++ closure <function2> (com.oreilly.learningsparkexamples.mini.scala.WordCount$$anonfun$1) is now cleaned +++
19/03/07 07:13:28 DEBUG ClosureCleaner: +++ Cleaning closure <function1> (org.apache.spark.rdd.RDD$$anonfun$saveAsTextFile$1$$anonfun$31) +++
19/03/07 07:13:28 DEBUG ClosureCleaner:  + declared fields: 1
19/03/07 07:13:28 DEBUG ClosureCleaner:      public static final long org.apache.spark.rdd.RDD$$anonfun$saveAsTextFile$1$$anonfun$31.serialVersionUID
19/03/07 07:13:28 DEBUG ClosureCleaner:  + declared methods: 2
19/03/07 07:13:28 DEBUG ClosureCleaner:      public final java.lang.Object org.apache.spark.rdd.RDD$$anonfun$saveAsTextFile$1$$anonfun$31.apply(java.lang.Object)
19/03/07 07:13:28 DEBUG ClosureCleaner:      public final scala.collection.Iterator org.apache.spark.rdd.RDD$$anonfun$saveAsTextFile$1$$anonfun$31.apply(scala.collection.Iterator)
19/03/07 07:13:28 DEBUG ClosureCleaner:  + inner classes: 1
19/03/07 07:13:28 DEBUG ClosureCleaner:      org.apache.spark.rdd.RDD$$anonfun$saveAsTextFile$1$$anonfun$31$$anonfun$apply$52
19/03/07 07:13:28 DEBUG ClosureCleaner:  + outer classes: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + outer objects: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + populating accessed fields because this is the starting closure
19/03/07 07:13:28 DEBUG ClosureCleaner:  + fields accessed by starting closure: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + there are no enclosing objects!
19/03/07 07:13:28 DEBUG ClosureCleaner:  +++ closure <function1> (org.apache.spark.rdd.RDD$$anonfun$saveAsTextFile$1$$anonfun$31) is now cleaned +++
19/03/07 07:13:28 DEBUG HadoopMapRedWriteConfigUtil: Saving as hadoop file of type (NullWritable, Text)
19/03/07 07:13:28 INFO deprecation: mapred.output.dir is deprecated. Instead, use mapreduce.output.fileoutputformat.outputdir
19/03/07 07:13:28 DEBUG FileCommitProtocol: Creating committer org.apache.spark.internal.io.HadoopMapRedCommitProtocol; job 5; output=file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/wordcounts; dynamic=false
19/03/07 07:13:28 DEBUG FileCommitProtocol: Falling back to (String, String) constructor
19/03/07 07:13:28 INFO HadoopMapRedCommitProtocol: Using output committer class org.apache.hadoop.mapred.FileOutputCommitter
19/03/07 07:13:28 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
19/03/07 07:13:28 DEBUG ClosureCleaner: +++ Cleaning closure <function2> (org.apache.spark.internal.io.SparkHadoopWriter$$anonfun$3) +++
19/03/07 07:13:28 DEBUG ClosureCleaner:  + declared fields: 6
19/03/07 07:13:28 DEBUG ClosureCleaner:      public static final long org.apache.spark.internal.io.SparkHadoopWriter$$anonfun$3.serialVersionUID
19/03/07 07:13:28 DEBUG ClosureCleaner:      private final org.apache.spark.internal.io.HadoopWriteConfigUtil org.apache.spark.internal.io.SparkHadoopWriter$$anonfun$3.config$1
19/03/07 07:13:28 DEBUG ClosureCleaner:      private final scala.reflect.ClassTag org.apache.spark.internal.io.SparkHadoopWriter$$anonfun$3.evidence$1$1
19/03/07 07:13:28 DEBUG ClosureCleaner:      private final int org.apache.spark.internal.io.SparkHadoopWriter$$anonfun$3.commitJobId$1
19/03/07 07:13:28 DEBUG ClosureCleaner:      private final java.lang.String org.apache.spark.internal.io.SparkHadoopWriter$$anonfun$3.jobTrackerId$1
19/03/07 07:13:28 DEBUG ClosureCleaner:      private final org.apache.spark.internal.io.HadoopMapReduceCommitProtocol org.apache.spark.internal.io.SparkHadoopWriter$$anonfun$3.committer$1
19/03/07 07:13:28 DEBUG ClosureCleaner:  + declared methods: 2
19/03/07 07:13:28 DEBUG ClosureCleaner:      public final java.lang.Object org.apache.spark.internal.io.SparkHadoopWriter$$anonfun$3.apply(java.lang.Object,java.lang.Object)
19/03/07 07:13:28 DEBUG ClosureCleaner:      public final org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage org.apache.spark.internal.io.SparkHadoopWriter$$anonfun$3.apply(org.apache.spark.TaskContext,scala.collection.Iterator)
19/03/07 07:13:28 DEBUG ClosureCleaner:  + inner classes: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + outer classes: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + outer objects: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + populating accessed fields because this is the starting closure
19/03/07 07:13:28 DEBUG ClosureCleaner:  + fields accessed by starting closure: 0
19/03/07 07:13:28 DEBUG ClosureCleaner:  + there are no enclosing objects!
19/03/07 07:13:28 DEBUG ClosureCleaner:  +++ closure <function2> (org.apache.spark.internal.io.SparkHadoopWriter$$anonfun$3) is now cleaned +++
19/03/07 07:13:28 INFO SparkContext: Starting job: runJob at SparkHadoopWriter.scala:78
19/03/07 07:13:28 DEBUG SortShuffleManager: Can't use serialized shuffle for shuffle 0 because we need to do map-side aggregation
19/03/07 07:13:28 INFO DAGScheduler: Registering RDD 3 (map at WordCount.scala:21)
19/03/07 07:13:28 INFO DAGScheduler: Got job 0 (runJob at SparkHadoopWriter.scala:78) with 2 output partitions
19/03/07 07:13:28 INFO DAGScheduler: Final stage: ResultStage 1 (runJob at SparkHadoopWriter.scala:78)
19/03/07 07:13:28 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 0)
19/03/07 07:13:28 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 0)
19/03/07 07:13:28 DEBUG DAGScheduler: submitStage(ResultStage 1)
19/03/07 07:13:28 DEBUG DAGScheduler: missing: List(ShuffleMapStage 0)
19/03/07 07:13:28 DEBUG DAGScheduler: submitStage(ShuffleMapStage 0)
19/03/07 07:13:28 DEBUG DAGScheduler: missing: List()
19/03/07 07:13:28 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[3] at map at WordCount.scala:21), which has no missing parents
19/03/07 07:13:28 DEBUG DAGScheduler: submitMissingTasks(ShuffleMapStage 0)
19/03/07 07:13:28 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 5.0 KB, free 366.0 MB)
19/03/07 07:13:28 DEBUG BlockManager: Put block broadcast_1 locally took  4 ms
19/03/07 07:13:28 DEBUG BlockManager: Putting block broadcast_1 without replication took  4 ms
19/03/07 07:13:28 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 2.9 KB, free 366.0 MB)
19/03/07 07:13:28 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 10.240.208.36:53247 (size: 2.9 KB, free: 366.3 MB)
19/03/07 07:13:28 DEBUG BlockManagerMaster: Updated info of block broadcast_1_piece0
19/03/07 07:13:28 DEBUG BlockManager: Told master about block broadcast_1_piece0
19/03/07 07:13:28 DEBUG BlockManager: Put block broadcast_1_piece0 locally took  13 ms
19/03/07 07:13:28 DEBUG BlockManager: Putting block broadcast_1_piece0 without replication took  16 ms
19/03/07 07:13:28 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1161
19/03/07 07:13:28 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[3] at map at WordCount.scala:21) (first 15 tasks are for partitions Vector(0, 1))
19/03/07 07:13:28 INFO TaskSchedulerImpl: Adding task set 0.0 with 2 tasks
19/03/07 07:13:28 DEBUG TaskSetManager: Epoch for TaskSet 0.0: 0
19/03/07 07:13:28 DEBUG TaskSetManager: Valid locality levels for TaskSet 0.0: NO_PREF, ANY
19/03/07 07:13:28 DEBUG TaskSchedulerImpl: parentName: , name: TaskSet_0.0, runningTasks: 0
19/03/07 07:13:28 DEBUG TaskSetManager: Valid locality levels for TaskSet 0.0: NO_PREF, ANY
19/03/07 07:13:28 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 7903 bytes)
19/03/07 07:13:28 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, executor driver, partition 1, PROCESS_LOCAL, 7903 bytes)
19/03/07 07:13:28 DEBUG TaskSetManager: No tasks for locality level NO_PREF, so moving to locality level ANY
19/03/07 07:13:28 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
19/03/07 07:13:28 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
19/03/07 07:13:28 INFO Executor: Fetching spark://10.240.208.36:53246/jars/learning-spark-mini-example_2.11-0.0.1.jar with timestamp 1551942806186
19/03/07 07:13:28 DEBUG TransportClientFactory: Creating new connection to /10.240.208.36:53246
19/03/07 07:13:29 DEBUG AbstractByteBuf: -Dio.netty.buffer.bytebuf.checkAccessible: true
19/03/07 07:13:29 DEBUG ResourceLeakDetectorFactory: Loaded default ResourceLeakDetector: io.netty.util.ResourceLeakDetector@6183baf5
19/03/07 07:13:29 DEBUG TransportClientFactory: Connection to /10.240.208.36:53246 successful, running bootstraps...
19/03/07 07:13:29 INFO TransportClientFactory: Successfully created connection to /10.240.208.36:53246 after 60 ms (0 ms spent in bootstraps)
19/03/07 07:13:29 DEBUG TransportClient: Sending stream request for /jars/learning-spark-mini-example_2.11-0.0.1.jar to /10.240.208.36:53246
19/03/07 07:13:29 DEBUG Recycler: -Dio.netty.recycler.maxCapacityPerThread: 32768
19/03/07 07:13:29 DEBUG Recycler: -Dio.netty.recycler.maxSharedCapacityFactor: 2
19/03/07 07:13:29 DEBUG Recycler: -Dio.netty.recycler.linkCapacity: 16
19/03/07 07:13:29 DEBUG Recycler: -Dio.netty.recycler.ratio: 8
19/03/07 07:13:29 INFO Utils: Fetching spark://10.240.208.36:53246/jars/learning-spark-mini-example_2.11-0.0.1.jar to /tmp/spark-afb7ffed-8bfe-47ab-9fa0-1f83e9e306e0/userFiles-60191de1-360e-47df-a134-40c29ce22bf5/fetchFileTemp7526697667016239322.tmp
19/03/07 07:13:29 INFO Executor: Adding file:/tmp/spark-afb7ffed-8bfe-47ab-9fa0-1f83e9e306e0/userFiles-60191de1-360e-47df-a134-40c29ce22bf5/learning-spark-mini-example_2.11-0.0.1.jar to class loader
19/03/07 07:13:29 DEBUG BlockManager: Getting local block broadcast_1
19/03/07 07:13:29 DEBUG BlockManager: Level for block broadcast_1 is StorageLevel(disk, memory, deserialized, 1 replicas)
19/03/07 07:13:29 INFO HadoopRDD: Input split: file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/README.md:1976+1976
19/03/07 07:13:29 INFO HadoopRDD: Input split: file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/README.md:0+1976
19/03/07 07:13:29 DEBUG HadoopRDD: Re-using cached JobConf
19/03/07 07:13:29 DEBUG HadoopRDD: Re-using cached JobConf
19/03/07 07:13:29 DEBUG TaskMemoryManager: Task 1 release 0.0 B from org.apache.spark.util.collection.ExternalSorter@642c0ae0
19/03/07 07:13:29 DEBUG TaskMemoryManager: Task 0 release 0.0 B from org.apache.spark.util.collection.ExternalSorter@5c2f6f48
19/03/07 07:13:29 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 1155 bytes result sent to driver
19/03/07 07:13:29 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1155 bytes result sent to driver
19/03/07 07:13:29 DEBUG TaskSchedulerImpl: parentName: , name: TaskSet_0.0, runningTasks: 1
19/03/07 07:13:29 DEBUG TaskSchedulerImpl: parentName: , name: TaskSet_0.0, runningTasks: 0
19/03/07 07:13:29 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 668 ms on localhost (executor driver) (1/2)
19/03/07 07:13:29 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 655 ms on localhost (executor driver) (2/2)
19/03/07 07:13:29 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool
19/03/07 07:13:29 DEBUG DAGScheduler: ShuffleMapTask finished on driver
19/03/07 07:13:29 DEBUG DAGScheduler: ShuffleMapTask finished on driver
19/03/07 07:13:29 INFO DAGScheduler: ShuffleMapStage 0 (map at WordCount.scala:21) finished in 0.885 s
19/03/07 07:13:29 INFO DAGScheduler: looking for newly runnable stages
19/03/07 07:13:29 INFO DAGScheduler: running: Set()
19/03/07 07:13:29 INFO DAGScheduler: waiting: Set(ResultStage 1)
19/03/07 07:13:29 INFO DAGScheduler: failed: Set()
19/03/07 07:13:29 DEBUG MapOutputTrackerMaster: Increasing epoch to 1
19/03/07 07:13:29 DEBUG DAGScheduler: submitStage(ResultStage 1)
19/03/07 07:13:29 DEBUG DAGScheduler: missing: List()
19/03/07 07:13:29 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[5] at saveAsTextFile at WordCount.scala:23), which has no missing parents
19/03/07 07:13:29 DEBUG DAGScheduler: submitMissingTasks(ResultStage 1)
19/03/07 07:13:29 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 72.5 KB, free 366.0 MB)
19/03/07 07:13:29 DEBUG BlockManager: Put block broadcast_2 locally took  2 ms
19/03/07 07:13:29 DEBUG BlockManager: Putting block broadcast_2 without replication took  3 ms
19/03/07 07:13:29 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 26.2 KB, free 365.9 MB)
19/03/07 07:13:29 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 10.240.208.36:53247 (size: 26.2 KB, free: 366.2 MB)
19/03/07 07:13:29 DEBUG BlockManagerMaster: Updated info of block broadcast_2_piece0
19/03/07 07:13:29 DEBUG BlockManager: Told master about block broadcast_2_piece0
19/03/07 07:13:29 DEBUG BlockManager: Put block broadcast_2_piece0 locally took  6 ms
19/03/07 07:13:29 DEBUG BlockManager: Putting block broadcast_2_piece0 without replication took  7 ms
19/03/07 07:13:29 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1161
19/03/07 07:13:29 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (MapPartitionsRDD[5] at saveAsTextFile at WordCount.scala:23) (first 15 tasks are for partitions Vector(0, 1))
19/03/07 07:13:29 INFO TaskSchedulerImpl: Adding task set 1.0 with 2 tasks
19/03/07 07:13:29 DEBUG TaskSetManager: Epoch for TaskSet 1.0: 1
19/03/07 07:13:29 DEBUG TaskSetManager: Valid locality levels for TaskSet 1.0: ANY
19/03/07 07:13:29 DEBUG TaskSchedulerImpl: parentName: , name: TaskSet_1.0, runningTasks: 0
19/03/07 07:13:29 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2, localhost, executor driver, partition 0, ANY, 7662 bytes)
19/03/07 07:13:29 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3, localhost, executor driver, partition 1, ANY, 7662 bytes)
19/03/07 07:13:29 INFO Executor: Running task 0.0 in stage 1.0 (TID 2)
19/03/07 07:13:29 INFO Executor: Running task 1.0 in stage 1.0 (TID 3)
19/03/07 07:13:29 DEBUG BlockManager: Getting local block broadcast_2
19/03/07 07:13:29 DEBUG BlockManager: Level for block broadcast_2 is StorageLevel(disk, memory, deserialized, 1 replicas)
19/03/07 07:13:29 DEBUG MapOutputTrackerMaster: Fetching outputs for shuffle 0, partitions 0-1
19/03/07 07:13:29 DEBUG MapOutputTrackerMaster: Fetching outputs for shuffle 0, partitions 1-2
19/03/07 07:13:29 DEBUG ShuffleBlockFetcherIterator: maxBytesInFlight: 50331648, targetRequestSize: 10066329, maxBlocksInFlightPerAddress: 2147483647
19/03/07 07:13:29 DEBUG ShuffleBlockFetcherIterator: maxBytesInFlight: 50331648, targetRequestSize: 10066329, maxBlocksInFlightPerAddress: 2147483647
19/03/07 07:13:29 INFO ShuffleBlockFetcherIterator: Getting 2 non-empty blocks including 2 local blocks and 0 remote blocks
19/03/07 07:13:29 INFO ShuffleBlockFetcherIterator: Getting 2 non-empty blocks including 2 local blocks and 0 remote blocks
19/03/07 07:13:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 15 ms
19/03/07 07:13:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 16 ms
19/03/07 07:13:29 DEBUG ShuffleBlockFetcherIterator: Start fetching local blocks: shuffle_0_0_1, shuffle_0_1_1
19/03/07 07:13:29 DEBUG ShuffleBlockFetcherIterator: Start fetching local blocks: shuffle_0_0_0, shuffle_0_1_0
19/03/07 07:13:29 DEBUG ShuffleBlockFetcherIterator: Got local blocks in  51 ms
19/03/07 07:13:29 DEBUG ShuffleBlockFetcherIterator: Got local blocks in  86 ms
19/03/07 07:13:29 INFO HadoopMapRedCommitProtocol: Using output committer class org.apache.hadoop.mapred.FileOutputCommitter
19/03/07 07:13:29 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
19/03/07 07:13:29 INFO HadoopMapRedCommitProtocol: Using output committer class org.apache.hadoop.mapred.FileOutputCommitter
19/03/07 07:13:29 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
19/03/07 07:13:30 DEBUG TaskMemoryManager: Task 2 release 0.0 B from org.apache.spark.util.collection.ExternalAppendOnlyMap@5801ec78
19/03/07 07:13:30 DEBUG OutputCommitCoordinator: Commit allowed for stage=1.0, partition=0, task attempt 0
19/03/07 07:13:30 INFO FileOutputCommitter: Saved output of task 'attempt_20190307071328_0005_m_000000_0' to file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/wordcounts/_temporary/0/task_20190307071328_0005_m_000000
19/03/07 07:13:30 INFO SparkHadoopMapRedUtil: attempt_20190307071328_0005_m_000000_0: Committed
19/03/07 07:13:30 INFO Executor: Finished task 0.0 in stage 1.0 (TID 2). 1502 bytes result sent to driver
19/03/07 07:13:30 DEBUG TaskSchedulerImpl: parentName: , name: TaskSet_1.0, runningTasks: 1
19/03/07 07:13:30 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 373 ms on localhost (executor driver) (1/2)
19/03/07 07:13:30 DEBUG TaskMemoryManager: Task 3 release 0.0 B from org.apache.spark.util.collection.ExternalAppendOnlyMap@486a79fa
19/03/07 07:13:30 DEBUG OutputCommitCoordinator: Commit allowed for stage=1.0, partition=1, task attempt 0
19/03/07 07:13:30 INFO FileOutputCommitter: Saved output of task 'attempt_20190307071328_0005_m_000001_0' to file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/wordcounts/_temporary/0/task_20190307071328_0005_m_000001
19/03/07 07:13:30 INFO SparkHadoopMapRedUtil: attempt_20190307071328_0005_m_000001_0: Committed
19/03/07 07:13:30 INFO Executor: Finished task 1.0 in stage 1.0 (TID 3). 1459 bytes result sent to driver
19/03/07 07:13:30 DEBUG TaskSchedulerImpl: parentName: , name: TaskSet_1.0, runningTasks: 0
19/03/07 07:13:30 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 3) in 417 ms on localhost (executor driver) (2/2)
19/03/07 07:13:30 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool
19/03/07 07:13:30 INFO DAGScheduler: ResultStage 1 (runJob at SparkHadoopWriter.scala:78) finished in 0.543 s
19/03/07 07:13:30 DEBUG DAGScheduler: After removal of stage 1, remaining stages = 1
19/03/07 07:13:30 DEBUG DAGScheduler: After removal of stage 0, remaining stages = 0
19/03/07 07:13:30 INFO DAGScheduler: Job 0 finished: runJob at SparkHadoopWriter.scala:78, took 1.905835 s
19/03/07 07:13:30 DEBUG FileOutputCommitter: Merging data from DeprecatedRawLocalFileStatus{path=file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/wordcounts/_temporary/0/task_20190307071328_0005_m_000000; isDirectory=true; modification_time=1551942810000; access_time=0; owner=; group=; permission=rwxrwxrwx; isSymlink=false} to file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/wordcounts
19/03/07 07:13:30 DEBUG FileOutputCommitter: Merging data from DeprecatedRawLocalFileStatus{path=file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/wordcounts/_temporary/0/task_20190307071328_0005_m_000000/part-00000; isDirectory=false; length=2103; replication=1; blocksize=33554432; modification_time=1551942810000; access_time=0; owner=; group=; permission=rw-rw-rw-; isSymlink=false} to file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/wordcounts/part-00000
19/03/07 07:13:30 DEBUG FileOutputCommitter: Merging data from DeprecatedRawLocalFileStatus{path=file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/wordcounts/_temporary/0/task_20190307071328_0005_m_000001; isDirectory=true; modification_time=1551942810000; access_time=0; owner=; group=; permission=rwxrwxrwx; isSymlink=false} to file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/wordcounts
19/03/07 07:13:30 DEBUG FileOutputCommitter: Merging data from DeprecatedRawLocalFileStatus{path=file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/wordcounts/_temporary/0/task_20190307071328_0005_m_000001/part-00001; isDirectory=false; length=1932; replication=1; blocksize=33554432; modification_time=1551942810000; access_time=0; owner=; group=; permission=rw-rw-rw-; isSymlink=false} to file:/home/vonzhou/spark-2.4.0-bin-hadoop2.7/wordcounts/part-00001
19/03/07 07:13:30 DEBUG HadoopMapRedCommitProtocol: Committing files staged for absolute locations Map()
19/03/07 07:13:30 INFO SparkHadoopWriter: Job job_20190307071328_0005 committed.
19/03/07 07:13:30 INFO SparkContext: Invoking stop() from shutdown hook
19/03/07 07:13:30 INFO SparkUI: Stopped Spark web UI at http://10.240.208.36:4040
19/03/07 07:13:30 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
19/03/07 07:13:30 INFO MemoryStore: MemoryStore cleared
19/03/07 07:13:30 INFO BlockManager: BlockManager stopped
19/03/07 07:13:30 INFO BlockManagerMaster: BlockManagerMaster stopped
19/03/07 07:13:30 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
19/03/07 07:13:30 INFO SparkContext: Successfully stopped SparkContext
19/03/07 07:13:30 INFO ShutdownHookManager: Shutdown hook called
19/03/07 07:13:30 INFO ShutdownHookManager: Deleting directory /tmp/spark-9e61f04c-4127-4305-9087-a57c3afe933e
19/03/07 07:13:30 INFO ShutdownHookManager: Deleting directory /tmp/spark-afb7ffed-8bfe-47ab-9fa0-1f83e9e306e0
```

查看执行后的结果：

```
vonzhou@ubuntu:~/spark-2.4.0-bin-hadoop2.7$ ls -alh wordcounts/
total 8.0K
drwxrwxrwx 1 vonzhou vonzhou 4.0K Mar  7 15:13 .
drwxr-xr-x 1 vonzhou vonzhou 4.0K Mar  7 15:13 ..
-rw-r--r-- 1 vonzhou vonzhou    8 Mar  7 15:13 ._SUCCESS.crc
-rw-r--r-- 1 vonzhou vonzhou   28 Mar  7 15:13 .part-00000.crc
-rw-r--r-- 1 vonzhou vonzhou   24 Mar  7 15:13 .part-00001.crc
-rw-r--r-- 1 vonzhou vonzhou    0 Mar  7 15:13 _SUCCESS
-rw-r--r-- 1 vonzhou vonzhou 2.1K Mar  7 15:13 part-00000
-rw-r--r-- 1 vonzhou vonzhou 1.9K Mar  7 15:13 part-00001
vonzhou@ubuntu:~/spark-2.4.0-bin-hadoop2.7$ wc -l wordcounts/part-00000
150 wordcounts/part-00000
vonzhou@ubuntu:~/spark-2.4.0-bin-hadoop2.7$ wc -l wordcounts/part-00001
144 wordcounts/part-00001
vonzhou@ubuntu:~/spark-2.4.0-bin-hadoop2.7$ tail -10 wordcounts/part-00001
(programs,2)
(option,1)
(package.),1)
(["Building,1)
(instance:,1)
(must,1)
(and,10)
(command,,2)
(system,1)
(Hadoop,3)
```

