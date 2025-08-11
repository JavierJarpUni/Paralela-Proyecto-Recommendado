// SparkRDDProcessor.scala
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.google.cloud.storage.{Storage, StorageOptions, BlobId, BlobInfo}
import scala.util.{Try, Success, Failure}

object SparkRDDProcessor {
  
  case class ProcessingResult(
    jobId: String,
    processingTime: Long,
    recordCount: Long,
    avgValue: Double,
    maxValue: Double,
    minValue: Double
  )
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BigData RDD Processor")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    val sc = new SparkContext(conf)
    
    try {
      val inputPath = args(0)
      val outputPath = args(1) 
      val jobId = args(2)
      
      val startTime = System.currentTimeMillis()
      
      // Cargar datos preprocessados desde Cloud Storage
      val dataRDD: RDD[Double] = sc.textFile(inputPath)
        .flatMap(line => Try(line.toDouble).toOption)
        .cache()
      
      // Procesamiento con RDD
      val count = dataRDD.count()
      val sum = dataRDD.reduce(_ + _)
      val avg = sum / count
      val max = dataRDD.max()
      val min = dataRDD.min()
      
      // Transformaciones adicionales
      val normalizedRDD = dataRDD.map(value => (value - avg) / math.sqrt(dataRDD.map(v => math.pow(v - avg, 2)).mean()))
      val categorizedRDD = normalizedRDD.map { value =>
        value match {
          case v if v > 1.0 => ("high", v)
          case v if v < -1.0 => ("low", v)
          case v => ("medium", v)
        }
      }
      
      // Agregaciones por categoría
      val categoryStats = categorizedRDD
        .groupByKey()
        .mapValues(values => {
          val valueList = values.toList
          (valueList.size, valueList.sum / valueList.size)
        })
        .collect()
      
      val endTime = System.currentTimeMillis()
      val processingTime = endTime - startTime
      
      val result = ProcessingResult(
        jobId = jobId,
        processingTime = processingTime,
        recordCount = count,
        avgValue = avg,
        maxValue = max,
        minValue = min
      )
      
      // Guardar resultados en Cloud Storage
      saveResultToGCS(result, categoryStats, outputPath, jobId)
      
      println(s"RDD Processing completed: $result")
      
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.exit(1)
    } finally {
      sc.stop()
    }
  }
  
  private def saveResultToGCS(result: ProcessingResult, 
                             categoryStats: Array[(String, (Int, Double))],
                             outputPath: String, 
                             jobId: String): Unit = {
    val storage = StorageOptions.getDefaultInstance.getService
    
    val resultJson = s"""{
      "job_id": "${result.jobId}",
      "processing_time_ms": ${result.processingTime},
      "record_count": ${result.recordCount},
      "avg_value": ${result.avgValue},
      "max_value": ${result.maxValue},
      "min_value": ${result.minValue},
      "category_stats": {
        ${categoryStats.map { case (cat, (count, avg)) => 
          s""""$cat": {"count": $count, "average": $avg}"""
        }.mkString(", ")}
      },
      "processing_type": "RDD"
    }"""
    
    val blobId = BlobId.of("bigdata-processing-results", s"spark_rdd/$jobId.json")
    val blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/json").build()
    storage.create(blobInfo, resultJson.getBytes)
  }
}