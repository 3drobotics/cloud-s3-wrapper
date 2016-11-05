import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.model.{HttpEntity, ContentTypes}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.scaladsl.{FileIO, Source, Sink}
import akka.testkit._
import akka.util.{ByteString, Timeout}
import com.amazonaws.AmazonClientException
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import io.dronekit.cloud.{S3URL, AWSWrapper, AWSException}
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
  implicit val logger = Logging.getLogger(system = system, logSource = getClass)
  val aws = new AWSWrapper()
  val s3url = S3URL(bucketName, "some_random_data.txt")
  implicit val timeout = 30 seconds

  val randomString = Random.alphanumeric.take(150000).mkString
  val randomStringIterator = randomString.iterator
  val data = HttpEntity(randomString).dataBytes

  "io.dronekit.cloud.S3UploadSink" should {
    "handle all messages" in {

      val res = Source.fromIterator(() => randomStringIterator).grouped(100).map(chunk => ByteString(chunk.mkString))
        .map{chunk => println(s"sending chunk of size ${chunk.length}"); chunk}
        .via(aws.multipartUploadTransform(s3url)).runWith(Sink.ignore)
      val result = Await.result(res, timeout)
      println(s"Result: $result")

      val randomDataRes = Await.result(aws.getObject(s3url), 60 seconds)
      assert(ByteString(randomDataRes).decodeString("US-ASCII") == randomString)
    }

    "handle a small upload" in {
      val url = getClass.getResource("/smallfile.txt")
      val file = new File(url.getFile)
      println(s"Reading file of size: ${file.length()}")
      val imageSource = FileIO.fromFile(file)

      val smallFile = S3URL(bucketName, "smallfile.txt")
      val res = imageSource.via(aws.multipartUploadTransform(smallFile)).runWith(Sink.ignore)
      Await.ready(res, timeout)
      // check for md5
      val metadataRes = Await.result(aws.getObjectMetadata(smallFile), 60 seconds)
      assert(metadataRes.getETag == "49ea9fa860f88a45545f5c59bc6dafbe-1")
    }

    "Upload using the Sink" in {
      val source = Source.repeat(ByteString("S3 Upload Test String 12345"))
        // Must be > 10 MB to test multiple parts
        .take(1024*1024)

      val fileName = S3URL(bucketName, "garbage_data.txt")
      val res = source.runWith(aws.streamUpload(fileName))
      Await.ready(res, 10 minutes)
      // check for md5
      val metadataRes = Await.result(aws.getObjectMetadata(fileName), 60 seconds)
      assert(metadataRes.getETag == "87ab7187c871e71a5ba430bf66523ff0-3")
    }

    "handle upload with retries" in {
      class FakeS3 extends AmazonS3Client {
        var partNum = 0

        override def uploadPart(uploadPartRequest: UploadPartRequest): UploadPartResult = {
          partNum = partNum + 1
          // make it fail every 5 parts
          if (partNum % 2 == 0) {
            throw new AmazonClientException(s"Don't feel like uploading part number $partNum")
          } else {
            super.uploadPart(uploadPartRequest)
          }
        }
      }
      val finnikyUpload = new AWSWrapper(new FakeS3())

      val randomDataUrl = S3URL(bucketName, "random_data_2.txt")
      val res = data.via(finnikyUpload.multipartUploadTransform(randomDataUrl)).runWith(Sink.ignore)

      Await.ready(res, timeout)
      // check for md5
      val randomDataRes = Await.result(aws.getObject(randomDataUrl), 60 seconds)
      assert(ByteString(randomDataRes).decodeString("US-ASCII") == randomString)
    }

    "abort on bad upload" in {
      class FakeS3 extends AmazonS3Client {
        var partNum = 0

        override def uploadPart(uploadPartRequest: UploadPartRequest): UploadPartResult = {
          throw new AmazonClientException(s"Don't feel like uploading part number $partNum")
        }

        override def abortMultipartUpload(abortMultipartUploadRequest: AbortMultipartUploadRequest): Unit =  {

        }
      }
      val badUpload = new AWSWrapper(new FakeS3())

      val res = data.via(badUpload.multipartUploadTransform(S3URL(bucketName, "random_data_3.txt"))).runWith(Sink.ignore)

      res.onComplete {
        case Success(x) => assert(false)
        case Failure(ex: AWSException) => assert(true)
        case Failure(ex: Throwable) => fail(ex)
      }

      Await.ready(res, timeout)

    }

  }

}
