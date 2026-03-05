error id: D50BD80EE01C177EDCA160E8512FA234
file:///C:/Users/RodrigoBlanco(AlfaFo/Desktop/ejemplo-spark/src/main/scala/example/Main.scala
### java.lang.StringIndexOutOfBoundsException: offset 9532, count -2, length 9764

occurred in the presentation compiler.



action parameters:
offset: 9539
uri: file:///C:/Users/RodrigoBlanco(AlfaFo/Desktop/ejemplo-spark/src/main/scala/example/Main.scala
text:
```scala
package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Dataset


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

      df.filter($"Open" > $"Close" && substring($"Date", 1, 4) === "2009").show()

      //Creamos la columna del año
      val year = substring($"Date", 1, 4)

      //Utilizar ventanas: decirle a Spark que "piense" el df en base a una partición y un orden
      val ventana = Window 
        .partitionBy(year)
        .orderBy($"Date")

      df.select($"Date", $"Volume")
        .withColumn("volumen_acumulado", 
            sum($"Volume").over(ventana)) //Vamos a ver el volumen acumulado cada día desde el principio del año
        .withColumn("Ranking", row_number().over(ventana)) //Contar las filas acumuladas
        .withColumn("Volumen anterior", lag($"Volume", 1, 0).over(ventana)) //Retroceder el valor en un registro
        .withColumn("Volumen siguiente", lead($"Volume", 1, 0).over(ventana)) //Subir el valor un registro
        .show() 

      //Ventana para calcular la media móvil de 5 días
      val ventanaMovil = Window 
        .partitionBy(year) //Partición
        .orderBy($"Date") //Orden
        .rowsBetween(-4, 0) //Frame

      val accionesMediaMovil = df
          .withColumn("MMA5", avg($"Close").over(ventanaMovil))

      accionesMediaMovil.show()

      //Calcular volumen medio anual y añadirlo como columna a la tabla
      val ventanaYear = Window.
        partitionBy(year)

      df.withColumn("Volumen Medio Anual", avg($"Volume").over(ventanaYear)).show()

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

    df.explain("formatted") //Muestra el physical plan formateado
    df.explain(true) //Muestra el Parsed, Analyzed, Optimized y Physical Plan

    volumenMedioAnual.explain(true) //Muestra lo mismo pero de la agregación realizada

  }

  def ejercicioDataFrame(): Unit = {

    val spark = SparkSession.builder()
      .appName("ejemplo-spark")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
        .option("header", true)
        .option("inferSchema", true)
        .csv("data/AAPL.csv")

    import spark.implicits._

    //Crear la ventana: indicación de cómo ordenar la BBDD
    val ventana = Window.orderBy($"Date")

    //Función lag para obtener el cierre del día anterior con la ventana de referencia
    val cierreAnterior = lag($"Adj Close",1,1).over(ventana)

    //Columnas pedidas en el ejercicio: rango, tendencia alcista o bajista, rendimiento y casteo del volumen
    val rango = $"High" - $"Low"
    val tendencia = when(cierreAnterior < $"Adj Close", "Alcista").otherwise("Bajista")
    val rendimiento  = ($"Adj Close" - cierreAnterior)/cierreAnterior
    val volumenLong = $"Volume".cast(LongType)


    //Volumen medio mensual
    val mes = substring($"Date", 6, 2)

    val volumenMensual = df
        .groupBy(mes.alias("Mes"))
        .agg(avg(volumenLong).alias("Volumen medio"))
        .orderBy(mes)

    volumenMensual.show()

    //Indicación del total de días alcistas y bajistas con volumen y rendimiento medio
    val tendencias = df
    .withColumns(Map("Rendimiento"-> rendimiento,
        "Tendencia"-> tendencia)) //Se agregan al df las variables que necesitan una ventana
      .groupBy($"Tendencia")
      .agg( count("*").alias("Total de dias"),
            avg(volumenLong).alias("Volumen medio"), 
            avg($"Rendimiento").alias("Rendimiento medio"))

    tendencias.show()

  }

  //Método para comprobar si es mejor invertir/salir cuando se produce un cruce de la MMA30
  def simularEstrategia(): Unit = {

    val spark = SparkSession.builder()
      .appName("ejemplo-spark")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/AAPL.csv")

    import spark.implicits._

    //Crear la ventana y añadir la media móvil a los datos
    val ventana30 = Window
      .orderBy($"Date")
      .rowsBetween(-29, 0)

    val dfMMA30 = df.withColumn("MMA30", avg($"Adj Close").over(ventana30))

    //Crear una columna de cruces: alcista (close(ayer) <= MMA30, close(hoy)> MMA30); bajista (close(ayer) >= MMA30, close(hoy) < MMA30)
    val ventanaFecha = Window.
      orderBy($"Date")

    val cierreAnterior = lag($"Adj@@")

    val dfEstrategia = dfMMA30
      .withColumn()


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


presentation compiler configuration:
Scala version: 2.12.18
Classpath:
<WORKSPACE>\.bloop\root\bloop-bsp-clients-classes\classes-Metals-2Vt6nKtcR7qWPv0tPowAKg== [exists ], <HOME>\AppData\Local\bloop\cache\semanticdb\com.sourcegraph.semanticdb-javac.0.11.2\semanticdb-javac-0.11.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\2.12.18\scala-library-2.12.18.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-core_2.12\3.5.6\spark-core_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-sql_2.12\3.5.6\spark-sql_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro\1.11.4\avro-1.11.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro-mapred\1.11.4\avro-mapred-1.11.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\twitter\chill_2.12\0.10.0\chill_2.12-0.10.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\twitter\chill-java\0.10.0\chill-java-0.10.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\xbean\xbean-asm9-shaded\4.23\xbean-asm9-shaded-4.23.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-client-api\3.3.4\hadoop-client-api-3.3.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-client-runtime\3.3.4\hadoop-client-runtime-3.3.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-launcher_2.12\3.5.6\spark-launcher_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-kvstore_2.12\3.5.6\spark-kvstore_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-network-common_2.12\3.5.6\spark-network-common_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-network-shuffle_2.12\3.5.6\spark-network-shuffle_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-unsafe_2.12\3.5.6\spark-unsafe_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-common-utils_2.12\3.5.6\spark-common-utils_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\javax\activation\activation\1.1.1\activation-1.1.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-recipes\2.13.0\curator-recipes-2.13.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\zookeeper\zookeeper\3.6.3\zookeeper-3.6.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\servlet\jakarta.servlet-api\4.0.3\jakarta.servlet-api-4.0.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-codec\commons-codec\1.17.0\commons-codec-1.17.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-compress\1.26.2\commons-compress-1.26.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-lang3\3.12.0\commons-lang3-3.12.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-math3\3.6.1\commons-math3-3.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-text\1.10.0\commons-text-1.10.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-io\commons-io\2.16.1\commons-io-2.16.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-collections\commons-collections\3.2.2\commons-collections-3.2.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-collections4\4.4\commons-collections4-4.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\code\findbugs\jsr305\3.0.2\jsr305-3.0.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\ning\compress-lzf\1.1.2\compress-lzf-1.1.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\xerial\snappy\snappy-java\1.1.10.5\snappy-java-1.1.10.5.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\lz4\lz4-java\1.8.0\lz4-java-1.8.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\github\luben\zstd-jni\1.5.5-4\zstd-jni-1.5.5-4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\roaringbitmap\RoaringBitmap\0.9.45\RoaringBitmap-0.9.45.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\modules\scala-xml_2.12\2.1.0\scala-xml_2.12-2.1.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\modules\scala-collection-compat_2.12\2.7.0\scala-collection-compat_2.12-2.7.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-reflect\2.12.18\scala-reflect-2.12.18.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-jackson_2.12\3.7.0-M11\json4s-jackson_2.12-3.7.0-M11.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-client\2.40\jersey-client-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-common\2.40\jersey-common-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-server\2.40\jersey-server-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\containers\jersey-container-servlet\2.40\jersey-container-servlet-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\containers\jersey-container-servlet-core\2.40\jersey-container-servlet-core-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\inject\jersey-hk2\2.40\jersey-hk2-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-all\4.1.96.Final\netty-all-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-epoll\4.1.96.Final\netty-transport-native-epoll-4.1.96.Final-linux-x86_64.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-epoll\4.1.96.Final\netty-transport-native-epoll-4.1.96.Final-linux-aarch_64.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-kqueue\4.1.96.Final\netty-transport-native-kqueue-4.1.96.Final-osx-aarch_64.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-kqueue\4.1.96.Final\netty-transport-native-kqueue-4.1.96.Final-osx-x86_64.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\clearspring\analytics\stream\2.9.6\stream-2.9.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-core\4.2.19\metrics-core-4.2.19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-jvm\4.2.19\metrics-jvm-4.2.19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-json\4.2.19\metrics-json-4.2.19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-graphite\4.2.19\metrics-graphite-4.2.19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-jmx\4.2.19\metrics-jmx-4.2.19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-databind\2.15.2\jackson-databind-2.15.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\module\jackson-module-scala_2.12\2.15.2\jackson-module-scala_2.12-2.15.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\ivy\ivy\2.5.1\ivy-2.5.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\oro\oro\2.0.8\oro-2.0.8.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\net\razorvine\pickle\1.3\pickle-1.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\net\sf\py4j\py4j\0.10.9.7\py4j-0.10.9.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-tags_2.12\3.5.6\spark-tags_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-crypto\1.1.0\commons-crypto-1.1.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\rocksdb\rocksdbjni\8.3.2\rocksdbjni-8.3.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\univocity\univocity-parsers\2.9.1\univocity-parsers-2.9.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-sketch_2.12\3.5.6\spark-sketch_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-catalyst_2.12\3.5.6\spark-catalyst_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\orc\orc-core\1.9.6\orc-core-1.9.6-shaded-protobuf.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\orc\orc-mapreduce\1.9.6\orc-mapreduce-1.9.6-shaded-protobuf.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hive\hive-storage-api\2.8.1\hive-storage-api-2.8.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-column\1.13.1\parquet-column-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-hadoop\1.13.1\parquet-hadoop-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-core\2.15.2\jackson-core-2.15.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\slf4j-api\2.0.7\slf4j-api-2.0.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro-ipc\1.11.4\avro-ipc-1.11.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\esotericsoftware\kryo-shaded\4.0.2\kryo-shaded-4.0.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-logging\commons-logging\1.1.3\commons-logging-1.1.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\fusesource\leveldbjni\leveldbjni-all\1.8\leveldbjni-all-1.8.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-annotations\2.15.2\jackson-annotations-2.15.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\crypto\tink\tink\1.9.0\tink-1.9.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\jul-to-slf4j\2.0.7\jul-to-slf4j-2.0.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\jcl-over-slf4j\2.0.7\jcl-over-slf4j-2.0.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\logging\log4j\log4j-slf4j2-impl\2.20.0\log4j-slf4j2-impl-2.20.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\logging\log4j\log4j-api\2.20.0\log4j-api-2.20.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\logging\log4j\log4j-core\2.20.0\log4j-core-2.20.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\logging\log4j\log4j-1.2-api\2.20.0\log4j-1.2-api-2.20.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-framework\2.13.0\curator-framework-2.13.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\zookeeper\zookeeper-jute\3.6.3\zookeeper-jute-3.6.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\yetus\audience-annotations\0.13.0\audience-annotations-0.13.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\roaringbitmap\shims\0.9.45\shims-0.9.45.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-core_2.12\3.7.0-M11\json4s-core_2.12-3.7.0-M11.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\ws\rs\jakarta.ws.rs-api\2.1.6\jakarta.ws.rs-api-2.1.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\external\jakarta.inject\2.6.1\jakarta.inject-2.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\annotation\jakarta.annotation-api\1.3.5\jakarta.annotation-api-1.3.5.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\osgi-resource-locator\1.0.3\osgi-resource-locator-1.0.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\validation\jakarta.validation-api\2.0.2\jakarta.validation-api-2.0.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-locator\2.6.1\hk2-locator-2.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\javassist\javassist\3.29.2-GA\javassist-3.29.2-GA.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-buffer\4.1.96.Final\netty-buffer-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-codec\4.1.96.Final\netty-codec-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-codec-http\4.1.96.Final\netty-codec-http-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-codec-http2\4.1.96.Final\netty-codec-http2-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-codec-socks\4.1.96.Final\netty-codec-socks-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-common\4.1.96.Final\netty-common-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-handler\4.1.96.Final\netty-handler-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-unix-common\4.1.96.Final\netty-transport-native-unix-common-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-handler-proxy\4.1.96.Final\netty-handler-proxy-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-resolver\4.1.96.Final\netty-resolver-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport\4.1.96.Final\netty-transport-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-classes-epoll\4.1.96.Final\netty-transport-classes-epoll-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-classes-kqueue\4.1.96.Final\netty-transport-classes-kqueue-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\thoughtworks\paranamer\paranamer\2.8\paranamer-2.8.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-sql-api_2.12\3.5.6\spark-sql-api_2.12-3.5.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\janino\janino\3.1.9\janino-3.1.9.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\janino\commons-compiler\3.1.9\commons-compiler-3.1.9.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\datasketches\datasketches-java\3.3.0\datasketches-java-3.3.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\orc\orc-shims\1.9.6\orc-shims-1.9.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\airlift\aircompressor\0.27\aircompressor-0.27.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\jetbrains\annotations\17.0.0\annotations-17.0.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\threeten\threeten-extra\1.7.1\threeten-extra-1.7.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-common\1.13.1\parquet-common-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-encoding\1.13.1\parquet-encoding-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-format-structures\1.13.1\parquet-format-structures-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-jackson\1.13.1\parquet-jackson-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\tukaani\xz\1.9\xz-1.9.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\esotericsoftware\minlog\1.3.0\minlog-1.3.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\objenesis\objenesis\2.5.1\objenesis-2.5.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\code\gson\gson\2.10.1\gson-2.10.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\protobuf\protobuf-java\3.19.6\protobuf-java-3.19.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\joda-time\joda-time\2.12.5\joda-time-2.12.5.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-client\2.13.0\curator-client-2.13.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-ast_2.12\3.7.0-M11\json4s-ast_2.12-3.7.0-M11.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-scalap_2.12\3.7.0-M11\json4s-scalap_2.12-3.7.0-M11.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\external\aopalliance-repackaged\2.6.1\aopalliance-repackaged-2.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-api\2.6.1\hk2-api-2.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-utils\2.6.1\hk2-utils-2.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\modules\scala-parser-combinators_2.12\2.3.0\scala-parser-combinators_2.12-2.3.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\antlr\antlr4-runtime\4.9.3\antlr4-runtime-4.9.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-vector\12.0.1\arrow-vector-12.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-memory-netty\12.0.1\arrow-memory-netty-12.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\datasketches\datasketches-memory\2.1.0\datasketches-memory-2.1.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\guava\guava\16.0.1\guava-16.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-format\12.0.1\arrow-format-12.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-memory-core\12.0.1\arrow-memory-core-12.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\datatype\jackson-datatype-jsr310\2.15.1\jackson-datatype-jsr310-2.15.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\flatbuffers\flatbuffers-java\1.12.0\flatbuffers-java-1.12.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-epoll\4.1.96.Final\netty-transport-native-epoll-4.1.96.Final.jar [exists ]
Options:
-Yrangepos -Xplugin-require:semanticdb




#### Error stacktrace:

```
java.base/java.lang.String.checkBoundsOffCount(String.java:4593)
	java.base/java.lang.String.rangeCheck(String.java:304)
	java.base/java.lang.String.<init>(String.java:300)
	scala.tools.nsc.interactive.Global.typeCompletions$1(Global.scala:1231)
	scala.tools.nsc.interactive.Global.completionsAt(Global.scala:1254)
	scala.meta.internal.pc.SignatureHelpProvider.$anonfun$treeSymbol$1(SignatureHelpProvider.scala:462)
	scala.Option.map(Option.scala:230)
	scala.meta.internal.pc.SignatureHelpProvider.treeSymbol(SignatureHelpProvider.scala:460)
	scala.meta.internal.pc.SignatureHelpProvider$MethodCall$.unapply(SignatureHelpProvider.scala:255)
	scala.meta.internal.pc.SignatureHelpProvider$MethodCallTraverser.visit(SignatureHelpProvider.scala:366)
	scala.meta.internal.pc.SignatureHelpProvider$MethodCallTraverser.traverse(SignatureHelpProvider.scala:360)
	scala.meta.internal.pc.SignatureHelpProvider$MethodCallTraverser.fromTree(SignatureHelpProvider.scala:329)
	scala.meta.internal.pc.SignatureHelpProvider.$anonfun$signatureHelp$3(SignatureHelpProvider.scala:33)
	scala.Option.flatMap(Option.scala:271)
	scala.meta.internal.pc.SignatureHelpProvider.$anonfun$signatureHelp$2(SignatureHelpProvider.scala:31)
	scala.Option.flatMap(Option.scala:271)
	scala.meta.internal.pc.SignatureHelpProvider.signatureHelp(SignatureHelpProvider.scala:29)
	scala.meta.internal.pc.ScalaPresentationCompiler.$anonfun$signatureHelp$1(ScalaPresentationCompiler.scala:434)
	scala.meta.internal.pc.CompilerAccess.withSharedCompiler(CompilerAccess.scala:148)
	scala.meta.internal.pc.CompilerAccess.$anonfun$withNonInterruptableCompiler$1(CompilerAccess.scala:132)
	scala.meta.internal.pc.CompilerAccess.$anonfun$onCompilerJobQueue$1(CompilerAccess.scala:209)
	scala.meta.internal.pc.CompilerJobQueue$Job.run(CompilerJobQueue.scala:152)
	java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
	java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	java.base/java.lang.Thread.run(Thread.java:842)
```
#### Short summary: 

java.lang.StringIndexOutOfBoundsException: offset 9532, count -2, length 9764