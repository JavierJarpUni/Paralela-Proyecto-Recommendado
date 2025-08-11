// SparkDataFrameProcessor.scala
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.google.cloud.storage.{Storage, StorageOptions, BlobId, BlobInfo}

object SparkDataFrameProcessor {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BigData DataFrame Processor")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()
    
    import spark.implicits._
    
    try {
      val inputPath = args(0)
      val outputPath = args(1)
      val jobId = args(2)
      
      val startTime = System.currentTimeMillis()
      
      // Cargar datos como DataFrame
      val schema = StructType(Array(
        StructField("value", DoubleType, true)
      ))
      
      val rawDF = spark.read
        .option("header", "false")
        .schema(schema)
        .csv(inputPath)
        .cache()
      
      // Estadísticas básicas
      val statsDF = rawDF.select(
        count("value").as("record_count"),
        avg("value").as("avg_value"),
        max("value").as("max_value"),
        min("value").as("min_value"),
        stddev("value").as("std_dev")
      )
      
      val stats = statsDF.collect()(0)
      val avgValue = stats.getAs[Double]("avg_value")
      val stdDev = stats.getAs[Double]("std_dev")
      
      // Normalización y categorización usando DataFrame API
      val normalizedDF = rawDF.withColumn(
        "normalized_value", 
        (col("value") - lit(avgValue)) / lit(stdDev)
      ).withColumn(
        "category",
        when(col("normalized_value") > 1.0, "high")
          .when(col("normalized_value") < -1.0, "low")
          .otherwise("medium")
      )
      
      // Agregaciones por categoría
      val categoryStatsDF = normalizedDF
        .groupBy("category")
        .agg(
          count("*").as("count"),
          avg("normalized_value").as("avg_normalized"),
          avg("value").as("avg_original")
        )
        .collect()
      
      val endTime = System.currentTimeMillis()
      val processingTime = endTime - startTime
      
      // Preparar resultado
      val result = Map(
        "job_id" -> jobId,
        "processing_time_ms" -> processingTime,
        "record_count" -> stats.getAs[Long]("record_count"),
        "avg_value" -> avgValue,
        "max_value" -> stats.getAs[Double]("max_value"),
        "min_value" -> stats.getAs[Double]("min_value"),
        "std_dev" -> stdDev,
        "category_stats" -> categoryStatsDF.map(row => 
          Map(
            "category" -> row.getAs[String]("category"),
            "count" -> row.getAs[Long]("count"),
            "avg_normalized" -> row.getAs[Double]("avg_normalized"),
            "avg_original" -> row.getAs[Double]("avg_original")
          )
        ).toList,
        "processing_type" -> "DataFrame"
      )
      
      // Guardar resultado
      saveResultToGCS(result, outputPath, jobId)
      
      println(s"DataFrame Processing completed for job: $jobId")
      
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
  
  private def saveResultToGCS(result: Map[String, Any], 
                             outputPath: String, 
                             jobId: String): Unit = {
    val storage = StorageOptions.getDefaultInstance.getService
    
    import org.json4s._
    import org.json4s.jackson.Serialization
    implicit val formats = DefaultFormats
    
    val resultJson = Serialization.write(result)
    
    val blobId = BlobId.of("bigdata-processing-results", s"spark_dataframe/$jobId.json")
    val blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/json").build()
    storage.create(blobInfo, resultJson.getBytes)
  }
}