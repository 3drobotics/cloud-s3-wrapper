import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Source, Sink}
import com.amazonaws.services.s3.AmazonS3Client
import io.dronekit.AWSWrapper

import scala.concurrent.ExecutionContextExecutor
import scala.language.postfixOps

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 8/17/15.
 *
 */
trait Service {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContextExecutor
  implicit val materializer: ActorMaterializer
  val logger: LoggingAdapter

  val s3Client = new AmazonS3Client()

  val routes = pathPrefix("upload") {
    post {
      extractRequest { request =>
        complete {
          println(s"Request is: ${request.entity.isChunked()}")
            val aws = new AWSWrapper( "com.3dr.publictest", "")

          request.entity.dataBytes.transform( () => aws.multipartUploadTransform("gimbaltest4k.mpeg")).runWith(Sink.ignore)
          StatusCodes.OK
        }
      }
    }
  }
}
