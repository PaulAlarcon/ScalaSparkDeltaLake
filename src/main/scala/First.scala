object First {
  import org.apache.spark.sql.SparkSession
  import io.delta.tables._


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Delta Lake Example")
      .master("local[*]")
//      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
//      .config(
//        "spark.sql.catalog.spark_catalog",
//        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
//      )
      .getOrCreate()

    val data_path = "/data/covid_vax_trend.csv"
    val delta_path = "/data/delta/covid_vax_trend"
    val vax_data_df = spark.read.option("header", true).format("csv").load(data_path)
    vax_data_df.write.format("parquet").mode("overwrite").save(delta_path)

//    val deltaTable = DeltaTable.convertToDelta(spark, "parquet.`/data/covid_vax_trend.csv`")
  }

}
