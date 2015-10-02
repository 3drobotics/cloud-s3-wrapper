import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.amazonaws.services.s3.AmazonS3Client
import io.dronekit.cloud.S3UploadSink

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
          val sink = Sink.actorSubscriber(S3UploadSink.props(s3Client, "com.3dr.publictest", "gimbaltest4k.mpeg"))
          request.entity.getDataBytes().runWith(sink, materializer)
          StatusCodes.OK
        }
      }
    }
  }
}
