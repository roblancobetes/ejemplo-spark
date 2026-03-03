package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}


object Main {
  def main(args: Array[String]): Unit = {

    UsoRDD()

  }


  def UsoRDD(): Unit = {

    val conf = new SparkConf()
      .setAppName("ejemplo-rdd")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    
    val rdd1 = sc.parallelize(1 to 100)

    println(rdd1.getNumPartitions)

    val rdd2 = rdd1.repartition(10) //Aumenta o disminuye las particiones (puede hacer shuffle)

    println(rdd2.getNumPartitions)

    val rdd3 = rdd1.coalesce(1) //Reduce particiones (sin shuffle por defecto)

    println(rdd3.getNumPartitions)

  }



  def usoDataFrame(): Unit = {

    val spark = SparkSession.builder()
      .appName("ejemplo-spark")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
        .option("header", value = true)
        .csv("data/AAPL.csv")

      df.show() //Mostrar las 10 primeras líneas del df

  }
}

