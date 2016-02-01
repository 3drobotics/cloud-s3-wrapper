import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Source, Sink}
import com.amazonaws.services.s3.AmazonS3Client
import io.dronekit.cloud.{S3URL, AWSWrapper}
import StatusCodes._

import scala.concurrent.ExecutionContextExecutor
import scala.language.postfixOps

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 8/17/15.
 *
 */
trait Service {
  implicit val system: ActorSystem
  implicit val executor: ExecutionContextExecutor
  implicit val materializer: ActorMaterializer
  implicit val logger: LoggingAdapter

  lazy val aws = new AWSWrapper()


  lazy val routes = pathPrefix("upload") {
    post {
      extractRequest { request =>
        val resultFuture = request.entity.dataBytes
          .via(aws.multipartUploadTransform(S3URL("com.3dr.publictest", "gimbaltest4k.mpeg"))).runWith(Sink.ignore)
        onComplete(resultFuture) {
          case scala.util.Success(result) =>
            println(s"Got result: $result")
            complete(OK)
          case scala.util.Failure(ex) =>
            logger.error(ex, "Failed to complete upload stream")
            complete(ex)
        }
      }
    }
  }
}
