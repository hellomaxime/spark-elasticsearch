package elasticsearch

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark.sql._

object SparkToElasticsearch {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkToES")
      .master("local[*]")
      .config("spark.es.nodes", "localhost")
      .config("spark.es.port", "9200")
      .getOrCreate()

    // Method 1 : rdd to dataframe
    val sc = spark.sparkContext

    val rdd = sc.parallelize(
      Seq(
        ("SDM", 2021, "OCHO"),
        ("Ninho", 2021, "Jefe"),
        ("Tiakola", 2022, "Melo")
      )
    )

    val dfWithDefaultSchema = spark.createDataFrame(rdd)
    dfWithDefaultSchema.printSchema()

    // createDataFrame overloaded method only accepts RDDs of type Row
    val rowRDD: RDD[Row] = rdd.map(x => Row(x._1, x._2, x._3))

    val schema = new StructType()
      .add(StructField("artist", StringType, false))
      .add(StructField("yearOfRelease", IntegerType, false))
      .add(StructField("albumName", StringType, false))

    val dfWithSchema: DataFrame = spark.createDataFrame(rowRDD, schema)
    dfWithSchema.printSchema()

    dfWithSchema.saveToEs("demoindex/albumindex")

    // Method 2 : using .toDF() implicit method
    val rdd2 = sc.parallelize(
      Seq(
        ("Adele", 2021, "30"),
        ("Doja Cat", 2021, "Planet Her")
      )
    )

    // import implicits from SparkSession
    import spark.implicits._
    val dfUsingToDFMethod = rdd2.toDF("artist", "yearOfRelease", "albumName")

    dfUsingToDFMethod.saveToEs("demoindex/albumindex")

    // Method 3 : same as method 2 but using scala case class

    // import spark.implicits._
    val dfAlbum = Seq(
      AlbumIndex("Dua Lipa", 2020, "Future Nostalgia"),
      AlbumIndex("Angele", 2021, "Nonante-Cinq"),
      AlbumIndex("Camila Cabello", 2018, "Camila")
    ).toDF

    dfAlbum.saveToEs("demoindex/albumindex")
  }
}

case class AlbumIndex(artist: String, yearOfRelease: Int, albumName: String)