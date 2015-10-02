import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.model.{HttpEntity, MediaTypes}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.io.SynchronousFileSource
import akka.stream.scaladsl.Sink
import akka.testkit._
import com.amazonaws.services.s3.AmazonS3Client
import io.dronekit.cloud.S3UploadSink
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 8/17/15.
 *
 */
class UploadTest extends WordSpec with Matchers with ScalatestRouteTest with Service {
  implicit def default(implicit system: ActorSystem) = RouteTestTimeout(5.seconds dilated)
  override val logger = Logging(system, getClass)
  val s3 = new AmazonS3Client()

  "io.dronekit.cloud.S3UploadSink" should {
    "handle all messages" in {
      val url = getClass.getResource("/gimbaltest4k.mpeg")
      val file = new File(url.getFile)
      println(s"Reading file of size: ${file.length()}")
      val imageSource = SynchronousFileSource(file)
      val res = imageSource
        .runWith(Sink.actorSubscriber(S3UploadSink.props(s3, "com.3dr.publictest", "gimbaltest4k.mpeg")))
      while (!res.isTerminated) Thread.sleep(100)
    }
  }

}
