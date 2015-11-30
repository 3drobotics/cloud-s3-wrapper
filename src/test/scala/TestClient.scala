import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 8/26/15.
 *
 */
object TestClient extends App {
  implicit val system = ActorSystem()
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()
  val logger = Logging(system, getClass)


  val file = new File("/Users/jasonmartens/Downloads/Star wars.mp4")
  logger.info(s"Reading file of size: ${file.length()}")
  val imageSource = Source.file(file)
  val entity = HttpEntity.Chunked.fromData(ContentTypes.`application/octet-stream`, imageSource)
  val request = HttpRequest(method = HttpMethods.POST, uri = "http://localhost:9090/upload", entity = entity)
  val responseFuture = Http().singleRequest(request)
  val result = Await.result(responseFuture, 45 seconds)
  logger.info(s"Result: $result")

  StdIn.readLine("done?")
  system.terminate()
}
