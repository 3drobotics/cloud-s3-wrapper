import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.model.{HttpEntity, MediaTypes}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.io.SynchronousFileSource
import akka.stream.scaladsl.{Flow, Sink}
import akka.testkit._
import akka.util.Timeout
import com.amazonaws.AmazonClientException
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import io.dronekit.{AWSException, AWSWrapper, S3UploadSink}
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Success, Failure}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 8/17/15.
 *
 */
class UploadTest extends WordSpec with Matchers with ScalatestRouteTest  {
  implicit def default(implicit system: ActorSystem) = RouteTestTimeout(5.seconds dilated)
  val bucketName = "com.3dr.publictest"
  implicit val logger = Logging.getLogger(system = system, logSource = getClass())
  val aws = new AWSWrapper(bucketName, "", logger)
  implicit val timeout = Timeout(200 seconds)



  "io.dronekit.cloud.S3UploadSink" should {
    "handle all messages" in {
      val url = getClass.getResource("/recap_res.obj.zip")
      val file = new File(url.getFile)
      println(s"Reading file of size: ${file.length()}")
      val imageSource = SynchronousFileSource(file)
      val res = imageSource
        .transform( () => aws.multipartUploadTransform("recap_res.obj.zip"))
        .runWith(Sink.ignore)

      Await.ready(res, timeout.duration)
      val metadataRes = Await.result(aws.getObjectMetadata("recap_res.obj.zip"), 60 seconds)
      assert(metadataRes.getETag == "4c412eb0b9ee1c7f628cbcf07ae9bbac-10")
    }

    "handle a small upload" in {
      val url = getClass.getResource("/smallfile.txt")
      val file = new File(url.getFile)
      println(s"Reading file of size: ${file.length()}")
      val imageSource = SynchronousFileSource(file)

      val res = imageSource
        .transform( () => aws.multipartUploadTransform("smallfile_2.txt"))
        .runWith(Sink.ignore)

      Await.ready(res, timeout.duration)
      // check for md5
      val metadataRes = Await.result(aws.getObjectMetadata("smallfile_2.txt"), 60 seconds)
      assert(metadataRes.getETag == "49ea9fa860f88a45545f5c59bc6dafbe-1")
    }

    "handle upload with retries" in {
      class FakeS3 extends AmazonS3Client {
        var partNum = 0

        override def uploadPart(uploadPartRequest: UploadPartRequest): UploadPartResult = {
          partNum = partNum + 1
          // make it fail every 5 parts
          if (partNum % 5 == 0) {
            throw new AmazonClientException(s"Don't feel like uploading part number ${partNum}")
          } else {
            super.uploadPart(uploadPartRequest)
          }
        }
      }
      val finnikyUpload = new AWSWrapper(bucketName, "", logger, new FakeS3())

      val url = getClass.getResource("/recap_res.obj.zip")
      val file = new File(url.getFile)
      println(s"Reading file of size: ${file.length()}")
      val imageSource = SynchronousFileSource(file)
      val res = imageSource
        .transform( () => finnikyUpload.multipartUploadTransform("recap_res_2.obj.zip"))
        .runWith(Sink.ignore)

      Await.ready(res, timeout.duration)
      // check for md5
      val metadataRes = Await.result(aws.getObjectMetadata("recap_res_2.obj.zip"), 60 seconds)
      assert(metadataRes.getETag == "4c412eb0b9ee1c7f628cbcf07ae9bbac-10")
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
      val badUpload = new AWSWrapper(bucketName, "", logger, new FakeS3())

      val url = getClass.getResource("/recap_res.obj.zip")
      val file = new File(url.getFile)
      println(s"Reading file of size: ${file.length()}")
      val imageSource = SynchronousFileSource(file)
        val res = imageSource
          .transform( () => badUpload.multipartUploadTransform("recap_res_3.obj.zip"))
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
