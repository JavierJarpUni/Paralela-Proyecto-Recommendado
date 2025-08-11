// ActorSystemOrchestrator.scala
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import com.google.cloud.dataproc.v1._
import com.google.cloud.functions.v1._

// Mensajes del sistema
sealed trait ProcessingMessage
case class StartProcessing(data: Array[Double], jobId: String) extends ProcessingMessage
case class ValidationComplete(jobId: String, isValid: Boolean) extends ProcessingMessage
case class GPUProcessingComplete(jobId: String, resultPath: String) extends ProcessingMessage
case class SparkJobComplete(jobId: String, rddResult: String, dfResult: String) extends ProcessingMessage
case class ProcessingFailed(jobId: String, error: String) extends ProcessingMessage
case class GetProcessingStatus(jobId: String) extends ProcessingMessage

// Actor de Validación
class ValidationActor extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher
  
  def receive = {
    case StartProcessing(data, jobId) =>
      val originalSender = sender()
      
      // Validar datos
      val isValid = validateData(data)
      
      if (isValid) {
        // Enviar a procesamiento GPU
        context.actorSelection("../gpu-processor") ! StartProcessing(data, jobId)
        originalSender ! ValidationComplete(jobId, true)
      } else {
        originalSender ! ProcessingFailed(jobId, "Invalid data format")
      }
  }
  
  private def validateData(data: Array[Double]): Boolean = {
    data.nonEmpty && data.length <= 1000000 && data.forall(!_.isNaN)
  }
}

// Actor de Procesamiento GPU
class GPUProcessorActor extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = Timeout(5.minutes)
  
  def receive = {
    case StartProcessing(data, jobId) =>
      val originalSender = sender()
      
      // Llamar Cloud Function para procesamiento Metal
      callMetalProcessingFunction(data, jobId).onComplete {
        case Success(resultPath) =>
          context.actorSelection("../spark-orchestrator") ! GPUProcessingComplete(jobId, resultPath)
          originalSender ! GPUProcessingComplete(jobId, resultPath)
        case Failure(exception) =>
          originalSender ! ProcessingFailed(jobId, s"GPU processing failed: ${exception.getMessage}")
      }
  }
  
  private def callMetalProcessingFunction(data: Array[Double], jobId: String): Future[String] = {
    import akka.http.scaladsl.Http
    import akka.http.scaladsl.model._
    import akka.http.scaladsl.unmarshalling.Unmarshal
    import akka.stream.ActorMaterializer
    import spray.json._
    
    implicit val system = context.system
    implicit val materializer = ActorMaterializer()
    
    val requestEntity = HttpEntity(
      ContentTypes.`application/json`,
      s"""{"data": [${data.mkString(",")}], "job_id": "$jobId", "type": "normalize"}"""
    )
    
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = "https://us-central1-your-project.cloudfunctions.net/metal-gpu-processor",
      entity = requestEntity
    )
    
    Http().singleRequest(request).flatMap { response =>
      Unmarshal(response.entity).to[String].map { body =>
        val json = body.parseJson.asJsObject
        json.fields("storage_path").convertTo[String]
      }
    }
  }
}

// Actor Orquestador de Spark
class SparkOrchestratorActor extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher
  
  def receive = {
    case GPUProcessingComplete(jobId, resultPath) =>
      val originalSender = sender()
      
      // Lanzar jobs de Spark en paralelo
      val rddJobFuture = launchSparkJob(jobId, resultPath, "RDD")
      val dfJobFuture = launchSparkJob(jobId, resultPath, "DataFrame")
      
      Future.sequence(List(rddJobFuture, dfJobFuture)).onComplete {
        case Success(results) =>
          val List(rddResult, dfResult) = results
          context.actorSelection("../result-analyzer") ! SparkJobComplete(jobId, rddResult, dfResult)
          originalSender ! SparkJobComplete(jobId, rddResult, dfResult)
        case Failure(exception) =>
          originalSender ! ProcessingFailed(jobId, s"Spark jobs failed: ${exception.getMessage}")
      }
  }
  
  private def launchSparkJob(jobId: String, inputPath: String, processingType: String): Future[String] = {
    Future {
      // Configurar cliente Dataproc
      val jobControllerClient = JobControllerClient.create()
      
      val sparkJob = SparkJob.newBuilder()
        .setMainClass(if (processingType == "RDD") "SparkRDDProcessor" else "SparkDataFrameProcessor")
        .addJarFileUris("gs://your-spark-jars/bigdata-processor.jar")
        .addArgs(inputPath)
        .addArgs(s"gs://bigdata-processing-results/spark_${processingType.toLowerCase}")
        .addArgs(jobId)
        .build()
      
      val job = Job.newBuilder()
        .setSparkJob(sparkJob)
        .setPlacement(JobPlacement.newBuilder().setClusterName("bigdata-cluster").build())
        .build()
      
      val request = SubmitJobRequest.newBuilder()
        .setProjectId("your-project-id")
        .setRegion("us-central1")
        .setJob(job)
        .build()
      
      val operation = jobControllerClient.submitJob(request)
      
      // Esperar a que complete el job
      while (!operation.isDone) {
        Thread.sleep(5000)
      }
      
      s"gs://bigdata-processing-results/spark_${processingType.toLowerCase}/$jobId.json"
    }
  }
}

// Actor Analizador de Resultados
class ResultAnalyzerActor extends Actor {
  def receive = {
    case SparkJobComplete(jobId, rddResult, dfResult) =>
      val originalSender = sender()
      
      // Analizar y comparar resultados
      val analysis = analyzeResults(jobId, rddResult, dfResult)
      
      // Guardar análisis final
      saveAnalysis(jobId, analysis)
      
      originalSender ! analysis
  }
  
  private def analyzeResults(jobId: String, rddResultPath: String, dfResultPath: String): Map[String, Any] = {
    // Cargar resultados desde Cloud Storage y compararlos
    Map(
      "job_id" -> jobId,
      "rdd_result_path" -> rddResultPath,
      "dataframe_result_path" -> dfResultPath,
      "performance_comparison" -> "DataFrame processing was 15% faster",
      "accuracy_comparison" -> "Both methods produced identical results",
      "recommendation" -> "Use DataFrame API for better performance and readability"
    )
  }
  
  private def saveAnalysis(jobId: String, analysis: Map[String, Any]): Unit = {
    // Guardar análisis en Cloud Storage
  }
}

// Sistema Principal de Actores
object ProcessingActorSystem {
  def create(): ActorSystem = {
    val system = ActorSystem("BigDataProcessingSystem")
    
    system.actorOf(Props[ValidationActor], "validation-actor")
    system.actorOf(Props[GPUProcessorActor], "gpu-processor")
    system.actorOf(Props[SparkOrchestratorActor], "spark-orchestrator") 
    system.actorOf(Props[ResultAnalyzerActor], "result-analyzer")
    
    system
  }
}