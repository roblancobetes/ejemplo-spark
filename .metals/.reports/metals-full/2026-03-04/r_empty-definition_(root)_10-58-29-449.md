error id: file:///C:/Users/RodrigoBlanco(AlfaFo/Desktop/ejemplo-spark/src/main/scala/example/Main.scala:org/apache/spark/sql/Dataset#withColumns().
file:///C:/Users/RodrigoBlanco(AlfaFo/Desktop/ejemplo-spark/src/main/scala/example/Main.scala
empty definition using pc, found symbol in pc: 
found definition using semanticdb; symbol org/apache/spark/sql/Dataset#withColumns().
empty definition using fallback
non-local guesses:

offset: 5740
uri: file:///C:/Users/RodrigoBlanco(AlfaFo/Desktop/ejemplo-spark/src/main/scala/example/Main.scala
text:
```scala
package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types._


object Main {
  def main(args: Array[String]): Unit = {

    usoDataFrame()

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
      //Llama al constructor de Stock
      Stock(cols(0), //Date
       cols(1).toDouble, //Open
       cols(2).toDouble, // High
       cols(3).toDouble, //Low
       cols(4).toDouble, //Close
       cols(5).toDouble, //adjClose
       cols(6).toLong) //Volume
      )

    print(header)
    datosTipados.take(20).foreach(println(_)) //Mostrar los primeros 20 registros

    //Buscar la sesión con mayor volumen
    val diaMovido = datosTipados.reduce((a,b) => if (a.volume > b.volume) a else b)
    println("\n" + "Este fue el dia con mayor volumen:")
    println(diaMovido + "\n")

    //Obtener el día en el que más subió el stock
    val mayorSubidaDiaria = datosTipados.reduce((a,b) => 
      if (a.close/a.open > b.close/b.open) a else b)
    println("\n" + "Aqui fue la mayor subida diaria")
    println(mayorSubidaDiaria + "\n")

    //Total de días con volumen mayor que 100M
    val diasVolumenAlto = datosTipados.filter(_.volume > 100000000).count()

    println("\n" + s"El total de dias con alto volumen es $diasVolumenAlto." + "\n")

    //Agrupaciones: volumen medio por año
    val volumenMedio = datosTipados 
      .map(s => (s.date.substring(0,4), s.volume))
      .groupByKey()
      .mapValues(vols => vols.sum/vols.size)

    //Lo mismo más eficiente
    val volumenMedio2 = datosTipados 
      .map(s => (s.date.substring(0,4), (s.volume, 1L))) //tomo pares de la forma (volumen, 1 long)
      .reduceByKey{ case ((vol1, c1), (vol2, c2)) => (vol1 + vol2, c1 + c2)} //En general: reduceByKey es más escalable que groupBy
      //Hace combinaciones locales en cada partición antes del shuffle
      .mapValues{ case (vol, c) => vol.toDouble/c }
      .coalesce(1) //Para que los datos se introduzcan en una partición y no interfiera con el orden: IMPORTANTE: datos ya agregados
      .sortBy(_._1)

    println("El volumen medio es:")
    volumenMedio2.foreach(println(_))

  }

  def usoDataFrame(): Unit = {

    val spark = SparkSession.builder()
      .appName("ejemplo-spark")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
        .option("header", value = true)
        .option("inferSchema", value = true)
        .csv("data/AAPL.csv")

      df.show() //Mostrar las 20 primeras líneas del df

      //Los dataframes no están tipados (tienen un tipado general)
      //Una fila de un df es un objeto tipo Row: tipo genérico de una fila
      //Lo más cercano al tipado es el Schema: Conjunto de características de las columnas
      df.printSchema()

      import spark.implicits._ //En este caso spark es la SparkSession. Implicits permite usar $

      //Operaciones con columnas en DFs
      df.select("Date","Open","Volume").show()
      df.select(col("Date"),$"Open",df("Volume")).show() //Versión con objetos tipo columna

      //Manejo de "objetos" tipo columna
      val columna = $"Open" //¡Falta df! Se evalúa en modo lazy => cuando se ejecute tendrá que tomar partido

      val nuevaColumna = columna + 2

      val columnaString = concat(columna.cast(StringType), lit("Hola"))

      df.select(columna, nuevaColumna.alias("Columna con suma"), columnaString.alias("Columna rara")).show(truncate = false)

      df.filter($"Open" > $"Close" && substring($"Date", 1, 4) === "2009" && abs($"Open" - $"Close")).show()

      //Día de mayor volumen
      val maxVol = df.agg(max($"Volume").alias("Max_Vol"))

      val maxVolDay = df.join(maxVol, df("Volume") === maxVol("Max_Vol"), "left")
          .select($"Date", $"Volume", $"Open", $"Close", maxVol("Max_Vol"))

      maxVolDay.show()

      val maxVolumeDay = df.
          orderBy(desc("Volume"))
          .select("Date", "Volume")
          .limit(1)

      val volumenMedioAnual = df
        .withColumn("Year", substring($"Date", 1, 4))
        .groupBy($"Year")
        .agg(avg($"Volume").alias("Vol_Medio"))
        .orderBy($"Year")

      volumenMedioAnual.show()

  }

  def ejercicioDataFrame(): Unit = {

    val spark = SparkSession.builder()
      .appName("ejemplo-spark")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
        .option("header", value = true)
        .option("inferSchema", value = true)
        .csv("data/AAPL.csv")

    import spark.implicits._
    val rango = 
    val nuevoDF = df.withC@@olumns{
      Map("Rango" -> ($"High" - $"Low"),
      "Tendencia" -> when($"Open" < $"Close", "Alcista").otherwise("Bajista"),
      )

    }


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