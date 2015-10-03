import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.model.{HttpEntity, MediaTypes}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.io.SynchronousFileSource
import akka.stream.scaladsl.Sink
import akka.testkit._
import com.amazonaws.services.s3.AmazonS3Client
import io.dronekit.cloud.{AWSWrapper, AWSClient, S3UploadSink}
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
  val aws = new AWSWrapper("com.3dr.publictest", "")

  "io.dronekit.cloud.S3UploadSink" should {
//    "handle all messages" in {
//      val url = getClass.getResource("/recap_res.obj.zip")
//      val file = new File(url.getFile)
//      println(s"Reading file of size: ${file.length()}")
//      val imageSource = SynchronousFileSource(file)
//      val res = imageSource
//        .runWith(Sink.actorSubscriber(S3UploadSink.props(s3, "com.3dr.publictest", "recap_res.obj.zip")))
//      while (!res.isTerminated) Thread.sleep(100)
//    }

    "handle a small upload" in {
      val url = getClass.getResource("/smallfile.txt")
      val file = new File(url.getFile)
      println(s"Reading file of size: ${file.length()}")
      val imageSource = SynchronousFileSource(file)

//      val sink = Sink.actorSubscriber(S3UploadSink.props(s3, "com.3dr.publictest", "smallfile.txt"))
      val testSink = aws.multipartUploadSink("smallfile.txt")
      val res = imageSource
        .runWith(testSink)


      while (!res.isTerminated) Thread.sleep(100)

      // check for md5
    }

    "handle a source error" in {
      pending
    }

    "verify that etags are correct" in {
      pending
    }
  }

}
