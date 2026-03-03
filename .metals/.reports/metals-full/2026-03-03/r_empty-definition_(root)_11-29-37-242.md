error id: file:///C:/Users/RodrigoBlanco(AlfaFo/Desktop/ejemplo-spark/src/main/scala/example/Main.scala:scala/Array#apply().
file:///C:/Users/RodrigoBlanco(AlfaFo/Desktop/ejemplo-spark/src/main/scala/example/Main.scala
empty definition using pc, found symbol in pc: 
found definition using semanticdb; symbol scala/Array#apply().
empty definition using fallback
non-local guesses:

offset: 1281
uri: file:///C:/Users/RodrigoBlanco(AlfaFo/Desktop/ejemplo-spark/src/main/scala/example/Main.scala
text:
```scala
package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}


object Main {
  def main(args: Array[String]): Unit = {

    UsoRddCsv()

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

  def UsoRddCsv(): Unit ={

    val spark = SparkSession.builder()
      .appName("ejemplo-spark")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext //Forma alternativa de crear un sparkContext

    val rdd = sc.textFile("data/AAPL.csv")

    val header = rdd.first()

    val data = rdd.filter(line => line != header)

    val datosSeparados = data.map{line => line.split(",")}

    //Los datos son: Date, Open, High, Low, Close, Adj Close, Volume
    val datosTipados = datosSeparados.map(cols => 
      Stock(fi@@la(0),
       fila(1).toDouble,
       fila(2).toDouble,
       fila(3).toDouble,
       fila(4).toDouble,
       fila(5).toDouble,
       fila(6).toLong)
      )

    print(header)
    datosTipados.take(20).foreach(lista => println(lista.mkString(", "))) //Mostrar los primeros 20 registros

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


case class Stock(
  date: String,
  open: Double,
  close: Double,
  high: Double,
  low: Double,
  adjClose: Double,
  volume: Long
)


```


#### Short summary: 

empty definition using pc, found symbol in pc: 