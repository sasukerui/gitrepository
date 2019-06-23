import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object sparktest {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\apache\\hadoop-2.7.1")
    // val spark = SparkSession.builder.appName("JavaWordCount").getOrCreate

    val conf = new SparkConf().setAppName("wordCount").setMaster("local");
    val sc = new SparkContext(conf)
    var array = Array(1 to 9)
    var string = ("sdf", "sdf", "fghfg")

    var rdd = sc.parallelize(1 to 9, 3)
    println(rdd.getNumPartitions)
    def testmap(x:Int):Int={
      x*3
    }
    var b = rdd.map(x => x * 3)
    rdd.map(testmap).collect().foreach(s=>println(s))
    val ints = b.collect()
    for (num <- ints) {
      println(num)
    }
    //mappartition
    rdd.mapPartitions(num => num.filter(_ >= 7))
    //rdd.flatMap()  对每一条输入进行处理，最后把多条数据合并
    def testflatmap(it:Iterator[Int]):Iterator[Int]={
      var list =List()
      while (it.hasNext){
        var a= it.next()
        list.::(a)
      }


      list.iterator
    }

    println(b.collect().mkString(","))

    //groupbykey
    var rdd1 = sc.parallelize(Array(("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)), 3)
    rdd1.groupByKey().collect().foreach(s => println(s))
    println("------------")
    //reudce
    rdd.reduce((x, y) => x + y)
    //mapValues
    rdd1.mapValues("x"+_+"c").collect().foreach(s=>println(s))
    //reducebykey
    rdd1.reduceByKey((x, y) => x - y).collect.foreach(s => println(s))

    //aggregateBykEY
    //rdd1.aggregateByKey()
    def testpartitioninde(index:Int,iter:Iterator[(Int,Int)]):Iterator[(Int,Int)]={
      iter.foreach(x=>println(index+"---"+x))
      iter
    }


    var list1=List(2,5,8,1,2,6,9,4,3,5)
    //par 使用了par函数将普通集合转为并行集合集合，通过多线程并发处理达到提升处理效率的目的
    list1.par.foreach(x=>println(x ))
    var aggregaterdd=sc.parallelize(list1,3)
    aggregaterdd.aggregate((0,0))( (acc, number) => (acc._1+number, acc._2+1),(par1, par2) => (par1._1+par2._1, par1._2+par2._2))
    // rdd1.mapPartitionsWithIndex(testpartitioninde,true).foreach(x=>println(x))
//aggregateByKey  参考 https://www.cnblogs.com/mstk/p/7000509.html
    var aggtest=Array((1,3),(1,2),(1,4),(2,3),(3,6),(3,8))
    var aggr=sc.parallelize(aggtest,2)
    aggr.mapPartitionsWithIndex(testpartitioninde,false).foreach(x=>println(x))
    aggr.aggregateByKey(3)(seqFunc,comFunc).foreach(x=>println(x))
    aggr.aggregateByKey(3)((a,b)=>math.max(a,b),(a,b)=>a+ b).foreach(s=>println(s))
//sortByKey
    aggr.sortByKey(false,3).foreach(s=>println(s))
    //join
    var joinrdd=sc.parallelize(Array((1,"hadoop"),(2,"spark"),(3,"kafka"),(1,"storm")),2)
    joinrdd.join(aggr).foreach(s=>println(s))
//coalesce  shuffle默认是false，主要作用是缩减partition数量，如果为true，可以扩容
    aggr.coalesce(1).foreach(s=>println(s))
    //repatition 调用colasece的true方法
    aggr.repartition(3).foreach(s=>println(s))
    println(aggr.count())
    println("TEST")
   sc.stop()
  }
  def seqFunc(a:Int,b:Int):Int= {
    //println(a,b)
     math.max(a, b)
  }
  def comFunc(a:Int,b:Int):Int={
    //println(a,b)
    a+b
  }

}
