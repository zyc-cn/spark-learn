package graphtest

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import util.SnowFlakeId

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

object IdMappingTest {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local[*]").appName("IdMappingTest").getOrCreate()
    val sparkContext = session.sparkContext
    todayIdMapping(sparkContext)
    session.stop()
  }

  def todayIdMapping(sparkContext: SparkContext): Unit = {
    val inputData: RDD[String] = sparkContext.textFile("input/id1.txt")
    // 每一行生成一个数组，每个数组中是一个唯一id和一个真实id
    val etlData: RDD[Array[(Long, String)]] = inputData.map(line => {
      val fields = line.split(",")
      fields.filter(!"null".equals(_)).map(item => (item.toLong , item))
    })
    // 缓存起来，防止重复计算
    etlData.cache()

    etlData.collect().foreach(item => println(item.mkString(" ")))
    // 清洗出所有的点（雪花id，保证分布式下的唯一性, value）
    val veritx: RDD[(Long, String)] = etlData.flatMap(item => item)

    // 生成所有点对应的边(因为每一行的元素之间才产生关系)
    val edges: RDD[Edge[String]] = etlData.flatMap(fields => {
      val allEdges = new ArrayBuffer[Edge[String]]()

      //将数据转换 将值转换成边 用于连线 连线值这边用"-"，具体的连接符想更换看个人意愿
      for (i <- 0 until fields.length - 1) {
        for (j <- i+1 until fields.length ) {
          // 用id去代表数据
          allEdges.+=(Edge(fields(i)._1, fields(j)._1, "-"))
        }
      }
      allEdges
    })

    edges.foreach(println)

    // 开始使用点集合与边集合进行图计算训练
    val graph: Graph[String, String] = Graph(veritx, edges)

    // 生成最大连通图
    val graph2 = graph.connectedComponents()
    val vertices = graph2.vertices

    vertices.foreach(println)

    // 将最小图计算值替换成uuid
    /**
     * 逻辑：
     * 1 vertices的数据结构就是（当前点-目标点）
     * 2 根据所有的目标点进行分组,并做出一个uuid作为全局id
     */
    val uidRdd = vertices.map(tp => (tp._2, tp._1))
      .groupByKey()
      .map(tp => (StringUtils.replace(UUID.randomUUID().toString, "-", ""), tp._2))

    uidRdd.foreach(println)

    //   对点与边进行统计作为记录输出 可以用于后期统计检查生成报表警报数据是否异常
    val uu = veritx.map(lin=>("vertices",1)).union(edges.map(lin=>("edges",1))).reduceByKey(_ + _)
      .map(tp=>tp._1+"\t"+tp._2)

    //    将现有的数据转换成铭文识别后展示
    //    将各个点的数据汇总到driver端
    val idmpMap = veritx.collectAsMap()
    //    按照map方式广播出去做转换
    val bc = sparkContext.broadcast(idmpMap)
    //  将数据的id转换成明文
    val ss = uidRdd.mapPartitions(itemap => {
      val vert_id_map = bc.value
      itemap.map(tp => {
        //从广播变量中获取id值的信息并转换
        val t2 = for (ele <- tp._2)  yield vert_id_map.get(ele).get
        //按照将要输出的数据格式进行排版 （uuid   mobile1,mobile2,mobile3,device_id1,device_2）
        tp._1+"\t"+t2.mkString(",")
      })
    })
    //  数据输出
    ss.foreach(println)
    uu.foreach(println)
  }
}
