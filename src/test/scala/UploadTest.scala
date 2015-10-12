import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.model.{HttpEntity, ContentTypes}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.io.SynchronousFileSource
import akka.stream.scaladsl.{Source, Sink}
import akka.testkit._
import akka.util.{ByteString, Timeout}
import com.amazonaws.AmazonClientException
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import io.dronekit.{AWSException, AWSWrapper}
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Random, Success, Failure}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 8/17/15.
 *
 */
class UploadTest extends WordSpec with Matchers with ScalatestRouteTest  {
  implicit def default(implicit system: ActorSystem) = RouteTestTimeout(5.seconds dilated)
  val bucketName = "com.3dr.publictest"
  implicit val logger = Logging.getLogger(system = system, logSource = getClass())
  val aws = new AWSWrapper(bucketName, "")
  implicit val timeout = Timeout(200 seconds)

  val randomString = Random.alphanumeric.take(15000000).mkString
  val databytes = HttpEntity(randomString).dataBytes

  "io.dronekit.cloud.S3UploadSink" should {
    "handle all messages" in {

      val res = databytes
        .transform( () => aws.multipartUploadTransform("some_random_data.txt"))
        .runWith(Sink.ignore)

      Await.ready(res, timeout.duration)

      val randomDataRes = Await.result(aws.getObject("some_random_data.txt"), 60 seconds)
      assert(ByteString(randomDataRes).decodeString("US-ASCII") == randomString)
    }

    "handle a small upload" in {
      val url = getClass.getResource("/smallfile.txt")
      val file = new File(url.getFile)
      println(s"Reading file of size: ${file.length()}")
      val imageSource = SynchronousFileSource(file)

      val res = imageSource
        .transform( () => aws.multipartUploadTransform("smallfile.txt"))
        .runWith(Sink.ignore)

      Await.ready(res, timeout.duration)
      // check for md5
      val metadataRes = Await.result(aws.getObjectMetadata("smallfile.txt"), 60 seconds)
      assert(metadataRes.getETag == "49ea9fa860f88a45545f5c59bc6dafbe-1")
    }

    "handle upload with retries" in {
      class FakeS3 extends AmazonS3Client {
        var partNum = 0

        override def uploadPart(uploadPartRequest: UploadPartRequest): UploadPartResult = {
          partNum = partNum + 1
          // make it fail every 5 parts
          if (partNum % 2 == 0) {
            throw new AmazonClientException(s"Don't feel like uploading part number ${partNum}")
          } else {
            super.uploadPart(uploadPartRequest)
          }
        }
      }
      val finnikyUpload = new AWSWrapper(bucketName, "", new FakeS3())

      val res = databytes
        .transform( () => finnikyUpload.multipartUploadTransform("random_data_2.txt"))
        .runWith(Sink.ignore)

      Await.ready(res, timeout.duration)
      // check for md5
      val randomDataRes = Await.result(aws.getObject("random_data_2.txt"), 60 seconds)
      assert(ByteString(randomDataRes).decodeString("US-ASCII") == randomString)
    }

    "abort on bad upload" in {
      class FakeS3 extends AmazonS3Client {
        var partNum = 0

        override def uploadPart(uploadPartRequest: UploadPartRequest): UploadPartResult = {
          throw new AmazonClientException(s"Don't feel like uploading part number ${partNum}")
        }

        override def abortMultipartUpload(abortMultipartUploadRequest: AbortMultipartUploadRequest): Unit =  {

        }
      }
      val badUpload = new AWSWrapper(bucketName, "", new FakeS3())

      val res = databytes
          .transform( () => badUpload.multipartUploadTransform("random_data_3.txt"))
          .runWith(Sink.ignore)

      res.onComplete {
        case Success(x) => assert(false)
        case Failure(ex: AWSException) => assert(true)
        case Failure(ex: Throwable) => assert(false)
      }

      Await.ready(res, timeout.duration)

    }

  }

}
