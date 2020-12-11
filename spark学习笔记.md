[TOC]

## 第一章：spark概述

* spark是一个快速（**基于内存**），通用，可扩展的集群**计算引擎**
  * **快速：**基于内存，比hadoop快100倍
  * **易用：**支持scala，java，python，r和sq脚本，并提供80+高性能算法，还可支持交互的python和scala的shell
  * **通用：**结合sql，streaming和复杂分析；提供大量类库（MLlib，GraphicX）
  * **可融合性：**非常方便与其他开源产品进行融合，和yarn，hdbs等合作

![img](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/10/%E5%9B%BE-BDAS%E6%9E%B6%E6%9E%84.jpg)

*yarn：调度计算资源（nodemanager，resourcemanager）*

*独立调度器是spark专用的调度器，需要给它配置单独的集群*

#### Spark Core

实现了spark的基本功能，包括任务调度、内存管理、错误恢复，与存储系统交互等模块。还包含了对<font color=blue>弹性分布式数据集（RDD)</font>的API定义

#### Spark SQL

####Spark Streaming

--------

### spark运行模式

* local模式
* standalone模式
* yarn模式
* Mesos模式

### 集群角色

#### 1. Master(leader)

负责资源调度，管理集群资源信息，类似yarn框架框架的resourcemanager【管理】

1. 监听work，看worker是否正常工作（心跳）
2. Master对Worker、application等的管理
   1. 接收worker的注册并管理所有的worker
   2. 接收client提交的application
   3. 调度等待的applicantion并向worker提交

#### 2.Worker(slave)

有多个，相当于slave，每个slave掌管着所在节点的资源信息，类似于yarn框架的nodemanager【干活】

1. 通过registerworker**注册到master**
2. 定时**发送心跳**给master
3. 根据master发送的application配置进程环境，并启动executorbackend（**执行task**所需的临时进程）

####  3.Driver和Executor

1. **Driver（驱动器）**

   spark的驱动器是执行开发程序中main方法的线程。

   用来**创建sparkcontext，创建RDD，以及进行RDD的转换操作和行动操作代码**的执行。

   主要负责：

    * **将用户程序转化为作业**（job）
    * 在executor之间**调度任务**（task）
    * 跟踪executor的**执行情况**
    * 通过ui展示查询运行情况

2. **executor（执行器）**

   Spark Executor是⼀个⼯作节点， 负责在 Spark 作业中运⾏任务， 任务间相互独⽴。 Spark 应⽤启动时， Executor 节点被同时启动， 并且始终伴随着整个 Spark 应⽤的⽣命周期⽽存在。 如果有Executor节点发⽣了故障或崩溃， Spark 应⽤也可以继续执⾏， 会将出错节点上的任务调度到其他Executor节
   点上继续运⾏。 主要负责： 

   1.  负责**运⾏组成 Spark 应⽤的任务**， 并将**状态信息返回给驱动器**程序；
   2.  通过⾃身的**块管理器（Block Manager）** 为⽤户程序中要求缓存的RDD**提供内存式存储**。 RDD是直接缓存在Executor内的， 因此任务可以在运⾏时充分利⽤缓存数据加速运算。 



------------------

### 架构设计

park运行架构包括集群资源管理器（Cluster Manager）（yarn，mesos等）、运行作业任务的工作节点（Worker Node）、每个应用的任务控制节点（Driver）和每个工作节点上负责具体任务的执行进程（Executor）

Spark所采用的Executor有两个优点：一是利用**多线程来执行具体的任务**（Hadoop MapReduce采用的是进程模型），<u>减少任务的启动开销；</u>二是Executor中有一个**BlockManager存储模块，会将内存和磁盘共同作为存储设备**，当需要多轮迭代计算时，可以将中间结果存储到这个存储模块里，下次需要时，就可以直接读该存储模块里的数据，而不需要读写到HDFS等文件系统里，因而有效<u>减少了IO开销</u>；或者在交互式查询场景下，预先将表缓存到该存储系统上，从而可以提高读写IO性能。

![img](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-5-Spark%E8%BF%90%E8%A1%8C%E6%9E%B6%E6%9E%84.jpg)

一个应用application，由一个任务控制节点driver和若干个作业job构成

一个作业由若干个阶段stage构成

一个阶段由若干个任务task组成

**当执行一个应用时，任务控制节点driver会向集群管理器cluster manager申请资源，启动executor，并向executor发送应用程序代码和文件，然后再executor上执行任务，运行结束后，执行结果返回给任务控制节点driver，或者写到hdfs或者其他数据库中。**

![img](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-6-Spark%E4%B8%AD%E5%90%84%E7%A7%8D%E6%A6%82%E5%BF%B5%E4%B9%8B%E9%97%B4%E7%9A%84%E7%9B%B8%E4%BA%92%E5%85%B3%E7%B3%BB.jpg)

### spark运行基本流程

1. 当一个spark应用被提交时，首先需要为这个应用构建起基本的运行环境，即由任务控制节点（Driver）**创建一个SparkContext**，由SparkContext负责和资源管理器（Cluster Manager）的通信以及进行资源的申请、任务的分配和监控等。**（1）SparkContext会向资源管理器注册并申请运行Executor的资源**
2. 资源管理器为Executor**（2）分配资源，并启动Executor进程**，Executor运行情况将随着“**心跳**”发送到资源管理器上；
3. SparkContext根据RDD的依赖关系**构建DAG图**，DAG图提交给DAG调度器（DAGScheduler）进行解析，将DAG图**分解成多个“阶段**”（每个阶段都是一个任务集），并且**计算出各个阶段之间的依赖关系**，然后把一个个“任务集”提交给底层的任务调度器（TaskScheduler）进行处理；
4. Executor向SparkContext**（3）申请任务**，任务调度器将**任务分发给Executor运行**，同时，SparkContext将**应用程序代码发放**给Executor；
5. 任务在Executor上运行，把**执行结果反馈**给任务调度器，然后反馈给DAG调度器，运行完毕后**写入数据**并**释放所有资源**。

![img](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-7-Spark%E8%BF%90%E8%A1%8C%E5%9F%BA%E6%9C%AC%E6%B5%81%E7%A8%8B%E5%9B%BE.jpg)

### spark运行框架特点：

1. 每个应用有自己专属executor进程，executor进程以**多线程方式**运行任务，减少了多进程任务频繁启动开销；
2. executor有**blockmanager**存储模块，使得中间结果不需要写入hdfs等文件系统，后续可直接读取，**提高IO性能**
3. 任务采用了数据本地性和推测执行等优化机制。数据本地性是尽量将计算移到数据所在的节点上进行，即**“计算向数据靠拢**”，因为移动计算比移动数据所占的网络资源要少得多。而且，Spark采用了**延时调度机制**，可以在更大的程度上实现执行过程优化。（比如，拥有数据的节点当前正被其他的任务占用，那么，在这种情况下是否需要将数据移动到其他的空闲节点呢？答案是不一定。因为，如果经过预测发现当前节点结束当前任务的时间要比移动数据的时间还要少，那么，调度就会等待，直到当前节点可用。）

### Spark入门：

#### 在pyspark中执行词频统计

1. 启动pyspark

2. 加载文件

   1. **加载本地文件**（注意：spark采用**惰性机制**，只有遇到行动类型的操作，才会从头到尾执行所有操作，因此，该条目录并不会马上执行显示结果）

      ```python
      textFile=sc.textFile('file:///usr/local/spark/mycode/wordcount/word.txt')
      #first()是一个“行动”（Action）类型的操作，会启动真正的计算过程，从文件中加载数据到变量textFile中，并取出第一行文本。
      line.first() 

      #正因为Spark采用了惰性机制，在执行转换操作的时候，即使我们输入了错误的语句，pyspark也不会马上报错，而是等到执行“行动”类型的语句时启动真正的计算，那个时候“转换”操作语句中的错误就会显示出来

      #如何把textFile变量中的内容再次写回到另外一个目录wordback
      textFile.saveAsTextFile("file:///usr/local/spark/mycode/wordcount/writeback")

      #如果重复执行上述操作，或文件目录已经存在，则会报错FileAlreadyExistsException: Output directory file:/usr/local/spark/mycode/wordcount/writeback already exists

      ```

   2. **加载hdfs中文件**

      ```python
      #如下三条语句都是等价的
      textFile = sc.textFile("hdfs://localhost:9000/user/hadoop/word.txt")
      textFile = sc.textFile("/user/hadoop/word.txt")
      textFile = sc.textFile("word.txt")
      textFile.first()

      #把textFile的内容写回到HDFS文件系统
      textFile.saveAsTextFile("writeback")
      ```

   3. **词频统计**

      ```python
      wordCount = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda a, b : a + b)

      wordCount.collect()

      #textFile包含了多行文本内容，textFile.flatMap(labmda line : line.split(” “))会遍历textFile中的每行文本内容，当遍历到其中一行文本内容时，会把文本内容赋值给变量line，并执行Lamda表达式line : line.split(” “)。line : line.split(” “)是一个Lamda表达式，左边表示输入参数，右边表示函数里面执行的处理逻辑，这里执行line.split(” “)，也就是针对line中的一行文本内容，采用空格作为分隔符进行单词切分，从一行文本切分得到很多个单词构成的单词集合。这样，对于textFile中的每行文本，都会使用Lamda表达式得到一个单词集合，最终，多行文本，就得到多个单词集合。textFile.flatMap()操作就把这多个单词集合“拍扁”得到一个大的单词集合。

      #针对这个大的单词集合，执行map()操作，也就是map(lambda word : (word, 1))，这个map操作会遍历这个集合中的每个单词，当遍历到其中一个单词时，就把当前这个单词赋值给变量word，并执行Lamda表达式word : (word, 1)，这个Lamda表达式的含义是，word作为函数的输入参数，然后，执行函数处理逻辑，这里会执行(word, 1)，也就是针对输入的word，构建得到一个tuple，形式为(word,1)，key是word，value是1（表示该单词出现1次）。

      #程序执行到这里，已经得到一个RDD，这个RDD的每个元素是(key,value)形式的tuple。

      #最后，针对这个RDD，执行reduceByKey(labmda a, b : a + b)操作，这个操作会把所有RDD元素按照key进行分组，然后使用给定的函数（这里就是Lamda表达式：a, b : a + b），对具有相同的key的多个value进行reduce操作，返回reduce后的(key,value)，比如(“hadoop”,1)和(“hadoop”,1)，具有相同的key，进行reduce以后就得到(“hadoop”,2)，这样就计算得到了这个单词的词频。
      ```

## 第二章：RDD详述

### RDD设计背景：

许多迭代式算法计算过程中会重用中间结果，但是mapreduce是将中间结果写入hdfs中，这带来了大量数据复制，磁盘IO和序列化开销，为此，RDD提供一个抽象的数据架构，我们不需要担心底层数据分布式特性，只需要将具体的**应用逻辑**表达为一系列**转换处理**，**不同RDD之间转换操作形成依赖关系**，可以实现**管道化**，从而避免了中间结果的存储，降低开销。

### RDD概念：

一个RDD就是一个分布式对象集合，本质上是一个**只读的分区记录集合**，每个RDD可以分成多个分区，每个分区就是一个数据集片段，并且**一个RDD的不同分区可以保存到集群中不同的节点上**，**从而可以在集群中不同节点进行并行计算**。

RDD提供了一种高度受限的共享内存模型，即**RDD是只读的记录分区的集合，不能直接修改**，只能基于**稳定的物理存储中**的数据集来创建RDD，或者通过在其他RDD上执行确定的**转换操作**（如map、join和groupBy）而创建得到新的RDD

### RDD数据运算：

1. #### **转换（Transformation）：**粗粒度的数据转换操作

   - 执行计算并指定输出的形式
   - （比如map、filter、groupBy、join等）
   - 接受RDD并返回RDD
2. #### **行动（Action）：**

   - 指定RDD之间的相互依赖关系
   - （比如count、collect等）
   - 接受RDD但是返回非RDD（即输出一个值或结果）

### RDD典型的执行过程如下：

1. RDD读入**外部数据源**（或者**内存中的集合**）进行创建；
2. DD经过一系列的“转换”操作，每一次都会产生不同的RDD，供给下一个“转换”使用；
3. 后一个RDD经“行动”操作进行处理，并输出到外部数据源

### [注意]

RDD采用了**惰性调用**，即在RDD的执行过程中（如图9-8所示），**真正的计算发生在RDD的“行动”操作**，对于“行动”之前的所有“转换”操作，Spark只是记录下“转换”操作应用的一些基础数据集以及RDD生成的轨迹，即相互之间的依赖关系，而不会触发真正的计算。

![图9-8 Spark的转换和行动操作](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-8-Spark%E7%9A%84%E8%BD%AC%E6%8D%A2%E5%92%8C%E8%A1%8C%E5%8A%A8%E6%93%8D%E4%BD%9C.jpg)

例如，在图9-9中，从输入中逻辑上生成A和C两个RDD，经过一系列“转换”操作，**逻辑上生成了F**（也是一个RDD），之所以说是逻辑上，是因为这时候计算并没有发生，Spark只是记录了RDD之间的生成和依赖关系。当F要进行输出时，也就是**当F进行“行动”操作**的时候，Spark才会**根据RDD的依赖关系生成DAG**，并从起点开始真正的计算。

![图9-9 RDD执行过程的一个实例](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-9-RDD%E6%89%A7%E8%A1%8C%E8%BF%87%E7%A8%8B%E7%9A%84%E4%B8%80%E4%B8%AA%E5%AE%9E%E4%BE%8B.jpg)

上述处理成为“血缘关系（Lineage）”，即DAG拓扑排序的结果。采用惰性调用，通过血缘关系连接起来的一系列RDD操作就可以实现**管道化**（pipeline）好处是：

1. 避免多次转换操作之间数据同步的等待
2. 使得管道中每次操作的计算变得相对简单，保证了每个操作在处理逻辑上的单一性

### RDD操作实例：

```python
#从HDFS文件中读取数据创建一个RDD；
fileRDD = sc.textFile('hdfs://localhost:9000/test.txt')

#定义一个过滤函数
def contains(line):
...     return 'hello world' in line

#对fileRDD进行转换操作得到一个新的RDD，即filterRDD
filterRDD = fileRDD.filter(contains)

#对filterRDD进行持久化，把它保存在内存或磁盘中
filterRDD.cache()

#count()是一个行动操作，用于计算一个RDD集合中包含的元素个数
filterRDD.count()
```

这个程序的执行过程如下：

*  创建这个Spark程序的执行上下文，即创建SparkContext对象；
* 从外部数据源（即HDFS文件）中读取数据创建fileRDD对象；
* 构建起fileRDD和filterRDD之间的依赖关系，形成DAG图，这时候并没有发生真正的计算，只是记录转换的轨迹；
* 执行到第6行代码时，count()是一个行动类型的操作，触发真正的计算，开始实际执行从fileRDD到filterRDD的转换操作，并把结果持久化到内存中，最后计算出filterRDD中包含的元素个数

### RDD特性

1. 高效容错性

   为了实现容错，需要再集群节点之间进行数据复制或者记录日志，即在节点之间之间会发生大量数据传输，这对于数据密集型用会带来很大开销。

   在RDD设计中，数据只读不可改，若要修改数据，必须从父RDD转换到子RDD，所以我们可以通过RDD血缘关系来**重新计算**得到丢失的分区来实现容错，而无需回滚整个系统，这就避免数据复制的高开销。重算过程可在不同节点之间并行进行，实现高效的容错。

   此外，RDD提供的转换操作都是粗粒度操作，而不记录具体数据和各种细粒度操作日志，降低了容错开销。

2. 中间结果持久化到内存

3. 存放的数据可以是java对象，避免了不必要的对象序列化和反序列化开销

### RDD之间依赖关系

**1.宽依赖**：一个父RDD的分区对应于一个子RDD的分区，或多个父RDD的分区对应于一个子RDD的分区

**2.窄依赖**：一个父RDD的一个分区对应一个子RDD的多个分区

![图9-10 窄依赖与宽依赖的区别](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-10-%E7%AA%84%E4%BE%9D%E8%B5%96%E4%B8%8E%E5%AE%BD%E4%BE%9D%E8%B5%96%E7%9A%84%E5%8C%BA%E5%88%AB.jpg)

*RDD数据集通过“血**缘关系”记住了它是如何从其它RDD中演变过来的**，血缘关系记录的是粗颗粒度的转换操作行为，当这个RDD的部分分区数据丢失时，它可以通过血缘关系获取足够的信息来重新运算和恢复丢失的数据分区，由此带来了性能的提升*

### 阶段划分

Spark通过分析各个RDD的依赖关系生成了DAG，再通过分析各个RDD中的分区之间的依赖关系来决定如何划分阶段

[待补充。。。]

### RDD运行过程

（1）创建RDD对象；
（2）SparkContext负责计算RDD之间的依赖关系，构建DAG；
（3）DAGScheduler负责把DAG图分解成多个阶段，每个阶段中包含了多个任务，每个任务会被任务调度器分发给各个工作节点（Worker Node）上的Executor去执行。

![图9-12 RDD在Spark中的运行过程](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-12-RDD%E5%9C%A8Spark%E4%B8%AD%E7%9A%84%E8%BF%90%E8%A1%8C%E8%BF%87%E7%A8%8B.jpg)

### RDD创建方式

1. 读取外部数据集

   * 从本地文件系统加载数据

     ```python
     lines = sc.textFile("file:///usr/local/spark/mycode/rdd/word.txt")
     ```

   * 从HDFS文件系统加载数据

     ```python
     #三种方法等价
     lines = sc.textFile("hdfs://localhost:9000/user/hadoop/word.txt")
     lines = sc.textFile("/user/hadoop/word.txt")
     lines = sc.textFile("word.txt")
     ```

2. 调用sparkcontext的parallelize方法，在driver中一个已经存在的集合上创建

```python
nums = [1,2,3,4,5]
rdd = sc.parallelize(nums)
```

### RDD操作

1. **转换（Transformation）**：基于现有的数据集创建一个新的数据集（逻辑上）
   * 每一次转换操作都会产生不同的RDD，供给下一个“转换”使用
   * 转换得到的RDD是惰性求值，即整个转换过程只记录了转换的轨迹，并不会发生真正的计算，只有发生真正的计算，开始从血缘关系源头开始，进行物理的转换操作
   * 下面列出一些常见的转换操作（Transformation API）：
     * **filter**(func)：筛选出满足函数func的元素，并返回一个新的数据集
     * **map**(func)：将每个元素传递到函数func中，并将结果返回为一个新的数据集
     * **flatMap**(func)：与map()相似，但每个输入元素都可以映射到0或多个输出结果
     * **groupByKey**()：应用于(K,V)键值对的数据集时，返回一个新的(K, Iterable)形式的数据集
     * **reduceByKey**(func)：应用于(K,V)键值对的数据集时，返回一个新的(K, V)形式的数据集，其中的每个值是将每个key传递到函数func中进行聚合
2. **行动（Action）**：在数据集上进行运算，返回计算值。
   * Spark程序执行到行动操作时，才会执行真正的计算，从文件中加载数据，完成一次又一次转换操作，最终，完成行动操作得到结果
   * 下面列出一些常见的行动操作（Action API）：
     * count() 返回数据集中的元素个数
     * collect() 以数组的形式返回数据集中的所有元素
     * first() 返回数据集中的第一个元素
     * take(n) 以数组的形式返回数据集中的前n个元素
     * reduce(func) 通过函数func（输入两个参数并返回一个值）聚合数据集中的元素
     * foreach(func) 将数据集中的每个元素传递到函数func中运行*

### 惰性机制

**转换操作**不会立即执行，而是记录转换轨迹，等到有**行动类型的操作**发生时，才会有触发真正的计算。此时，spark会将计算分解成多个任务在不同机器上执行，每个机器运行属于它自己的map和reduce，最后将结果返回driver program

```python
lines = sc.textFile("file:///usr/local/spark/mycode/rdd/word.txt")
lines.map(lambda line : len(line.split(" "))).reduce(lambda a,b : (a > b and a or b))	
#lines是个string类型RDD
#lines.map()，是一个转换操作
#map(func)：将每个元素传递到函数func中，并将结果返回为一个新的数据集
#len(line.split(” “))这个处理逻辑的功能是，对line文本内容进行单词切分，得到很多个单词构成的集合
#终lines.map(lambda line : len(line.split(” “)))转换操作得到的RDD，是一个整型RDD，里面每个元素都是整数值
```

### 持久化

由于spark中，RDD采用惰性求值，即每次遇到行动操作，都会从头开始执行就散，所以可能代价很大。因此，我们可以使用持久化（缓存）机制避免这种重复计算的开销。

使用**persist（）语句**，当遇到第一个行动操作触发计算后，才会把计算结果进行持久化，然后其持久化RDD将会**保留在计算节点的内存**中被后面的行动操作重复使用。

persist()的圆括号中包含的是持久化级别参数，比如，persist(MEMORY_ONLY)表示将RDD作为反序列化的对象存储于JVM中，如果内存不足，就要**按照LRU原则替换缓存中的内容。**

persist(MEMORY_AND_DISK)表示将RDD作为反序列化的对象存储在JVM中，如果内存不足，**超出的分区将会被存放在硬盘上。**

*一般而言，使用cache()方法时，会调用persist(MEMORY_ONLY)*

```python
list = ["Hadoop","Spark","Hive"]
rdd = sc.parallelize(list)
rdd.cache()  //会调用persist(MEMORY_ONLY)，但是，语句执行到这里，并不会缓存rdd，这是rdd还没有被计算生成
print(rdd.count()) //第一次行动操作，触发一次真正从头到尾的计算，这时才会执行上面的rdd.cache()，把这个rdd放到缓存中
3
print(','.join(rdd.collect())) //第二次行动操作，不需要触发从头到尾的计算，只需要重复使用上面缓存中的rdd
#Hadoop,Spark,Hive
```

最后，可以使用unpersist()方法手动地把持久化的RDD从缓存中移除。

### 分区

RDD是弹性分布式数据集，通常RDD很大，会被分成很多个分区，分别保存在不同的节点上。

分区原则是使得分区的个数尽量等于集群中的CPU核心（core）数目。

对于不同的Spark部署模式而言（本地模式、Standalone模式、YARN模式、Mesos模式），都可以通过设置**spark.default.parallelism**这个参数的值，来配置默认的分区数目，一般而言：

* 本地模式：默认为本地机器的CPU数目，若设置了local[N],则默认为**N**；
* Apache Mesos：默认的分区数为**8**；
* Standalone或YARN：在“集群中所有CPU核心数目总和”和“2”二者中取**较大值**作为默认值；

因此，对于**parallelize**而言，如果没有在方法中指定分区数，则默认为spark.default.parallelism，比如：

```python
array = [1,2,3,4,5]
rdd = sc.parallelize(array,2) #设置两个分区
```

对于textFile而言，如果没有在方法中指定分区数，则默认为min(defaultParallelism,2)，其中，defaultParallelism对应的就是spark.default.parallelism。
如果是从HDFS中读取文件，则分区数为**文件分片数**(比如，128MB/片)。

#### 打印元素

**rdd.foreach(print)或rdd.map(print)**

**本地模式（local）**：会将所有元素都打印

**集群模式**：worker节点执行打印语句是输出到worker节点的stdout，而不是输出到任务控制节点driver program，为此，可以使用collect（）方法，如**rdd.collect().foreach(print)**,但是此方法会将worker节点所有rdd元素都抓取到driver program中，可能导致内存溢出，因此，我们只需打印RDD部分元素，**rdd.take(100).foreach(print)**

###  键值对RDD

一种常见的RDD元素类型

1. **创建键值对RDD**：

   1. 从文件中加载

      ```python
      lines = sc.textFile("file:///usr/local/spark/mycode/pairrdd/word.txt")
      pairRDD = lines.flatMap(lambda line : line.split(" ")).map(lambda word : (word,1))
      #将每行进行分割，生成多个单词集合，flatMap将单词集合合并为一个单词集合
      #map是将值输入操作中，得到新的RDD-->键值对（'word',1)
      ```

   2. 通过并行集合（列表）创建RDD

      ```python
      list = ["Hadoop","Spark","Hive","Spark"]
      rdd = sc.parallelize(list)
      pairRDD = rdd.map(lambda word : (word,1))
      pairRDD.foreach(print)
      ```

### 常用的键值对转换操作

##### **reduceByKey(func)** [重要]

使用func函数合并具有相同键的值。比如，reduceByKey((a,b) => a+b)，有四个键值对(“spark”,1)、(“spark”,2)、(“hadoop”,3)和(“hadoop”,5)，对具有相同key的键值对进行合并后的结果就是：(“spark”,3)、(“hadoop”,8)。可以看出，(a,b) => a+b这个Lamda表达式中，a和b都是指value，比如，对于两个具有相同key的键值对(“spark”,1)、(“spark”,2)，a就是1，b就是2。

```python
pairRDD.reduceByKey(lambda a,b : a+b).foreach(print)
(Spark,2)
(Hive,1)
(Hadoop,1)
```

##### groupByKey()

对具有相同键的值进行分组。比如，对四个键值对(“spark”,1)、(“spark”,2)、(“hadoop”,3)和(“hadoop”,5)，采用groupByKey()后得到的结果是：(“spark”,(1,2))和(“hadoop”,(3,5))。

##### keys()&values()

略

##### sortByKey()

##### mapValues(func) [重要]

对键值对RDD中的每个value都应用一个函数，但是，key不会发生变化。比如，对四个键值对(“spark”,1)、(“spark”,2)、(“hadoop”,3)和(“hadoop”,5)构成的pairRDD，如果执行pairRDD.mapValues(lambda x : x+1)，就会得到一个新的键值对RDD，它包含下面四个键值对(“spark”,2)、(“spark”,3)、(“hadoop”,4)和(“hadoop”,6)。

```python
pairRDD.mapValues( lambda x : x+1).foreach(print)
(Hadoop,2)
(Spark,2)
(Hive,2)
(Spark,2)
```

##### join

类似关系数据库，包括内连接，左外连接，右外连接等，join是内连接

比如，pairRDD1是一个键值对集合{(“spark”,1)、(“spark”,2)、(“hadoop”,3)和(“hadoop”,5)}，pairRDD2是一个键值对集合{(“spark”,”fast”)}，那么，pairRDD1.join(pairRDD2)的结果就是一个新的RDD，这个新的RDD是键值对集合{(“spark”,1,”fast”),(“spark”,2,”fast”)}。对

```python
pairRDD1 = sc.parallelize([('spark',1),('spark',2),('hadoop',3),('hadoop',5)])
pairRDD2 = sc.parallelize([('spark','fast')])
pairRDD1.join(pairRDD2)
```

### 综合实例

```python
rdd = sc.parallelize([("spark",2),("hadoop",6),("hadoop",4),("spark",6)])
rdd.mapValues(lambda x : (x,1)).reduceByKey(lambda x,y : (x[0]+y[0],x[1] + y[1])).mapValues(lambda x : (x[0] / x[1])).collect()
#mapValues，把每个键值对中的值添加一个记录1，即（“spark”，（2,1））
#reduceByKey，按照相同key，对value进行求和，即（“spark”，（8,2））
#mapValues，计算values中平均数
```

--------------

### 共享变量

当spark在集群的多个不同节点的多个任务上并行运行一个函数时，它会把函数中涉及到的每个变量，在每个任务上都生成一个副本。但是，有时候，需要在**多个任务之间共享变量**，或者在**任务（Task）和任务控制节点（Driver Program）之间共享变量**，为此，spark提供了两种类型的变量：**广播变量（broadcast variables）和累加器（accumulators）**

#### 广播变量

在每个机器上缓存一个只读的变量，而不是为机器上的每个任务都生成一个副本。通过这种方式，就可以非常高效地给每个节点（机器）提供一个大的输入数据集的副本

#### 累加器















































### spark支持的三种集群部署方式：（可不看）

1. standalone（独立集群管理器）

   - spark自带资源调度管理服务，可以独立部署到一个集群中，而不需要依赖其他系统为其提供资源管理调度服务。
   - 由一个Master和若干个Slave构成，并且以槽（slot）作为资源分配单位

2. yarn

   ![img](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-13-Spark-on-Yarn%E6%9E%B6%E6%9E%84.jpg)

3. mesos

### 从“Hadoop+Storm”架构转向Spark架构

为了能同时进行批处理与流处理，企业应用中通常会采用“Hadoop+Storm”的架构（也称为Lambda架构）

Hadoop负责对批量历史数据的实时查询和离线分析，而Storm则负责对流数据的实时处理。

![img](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-14-%E9%87%87%E7%94%A8HadoopStorm%E9%83%A8%E7%BD%B2%E6%96%B9%E5%BC%8F%E7%9A%84%E4%B8%80%E4%B8%AA%E6%A1%88%E4%BE%8B.jpg)



由于上述架构部署较为繁琐，由于spark同时支持批处理和流处理，所以spark是一个很好选择

但是，一方面spark streaming是将流数据分解成一系列短小的批处理作业，这些短小批处理作业使用面向批处理的sparkcore进行处理，即并不是真正的实时流计算，因而无法实现毫秒级的响应。

![img](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-15-%E7%94%A8Spark%E6%9E%B6%E6%9E%84%E6%BB%A1%E8%B6%B3%E6%89%B9%E5%A4%84%E7%90%86%E5%92%8C%E6%B5%81%E5%A4%84%E7%90%86%E9%9C%80%E6%B1%82.jpg)

当然，spark无法完全取代hadoop，同时将hadoop的项目转移到spark需要一定成本，因此对hadoop和spark统一部署，较为合理。

好处：

1. 计算资源按需伸缩；
2. 不用负载应用混搭，集群利用率高；
3. 共享底层存储，避免数据跨集群迁移。

![img](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-16-Hadoop%E5%92%8CSpark%E7%9A%84%E7%BB%9F%E4%B8%80%E9%83%A8%E7%BD%B2.jpg)

### 