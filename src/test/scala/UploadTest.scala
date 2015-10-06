import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.model.{HttpEntity, MediaTypes}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.io.SynchronousFileSource
import akka.stream.scaladsl.Sink
import akka.testkit._
import com.amazonaws.AmazonClientException
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import io.dronekit.{AWSException, AWSWrapper, S3UploadSink}
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
  val bucketName = "com.3dr.publictest"
  val aws = new AWSWrapper(bucketName, "")

  "io.dronekit.cloud.S3UploadSink" should {
//    "handle all messages" in {
//      val url = getClass.getResource("/recap_res.obj.zip")
//      val file = new File(url.getFile)
//      println(s"Reading file of size: ${file.length()}")
//      val imageSource = SynchronousFileSource(file)
//      val res = imageSource
//        .runWith(aws.multipartUploadSink("recap_res.obj.zip"))
//      while (!res.isTerminated) Thread.sleep(100)
//
//      val metadataRes = Await.result(aws.getObjectMetadata("recap_res.obj.zip"), 60 seconds)
//      assert(metadataRes.getETag == "4c412eb0b9ee1c7f628cbcf07ae9bbac-10")
//    }
//
//    "handle a small upload" in {
//      val url = getClass.getResource("/smallfile.txt")
//      val file = new File(url.getFile)
//      println(s"Reading file of size: ${file.length()}")
//      val imageSource = SynchronousFileSource(file)
//
//      val testSink = aws.multipartUploadSink("smallfile.txt")
//      val res = imageSource
//        .runWith(testSink)
//
//      while (!res.isTerminated) Thread.sleep(100)
//
//      // check for md5
//      val metadataRes = Await.result(aws.getObjectMetadata("smallfile.txt"), 60 seconds)
//      assert(metadataRes.getETag == "49ea9fa860f88a45545f5c59bc6dafbe-1")
//    }
//
//    "handle upload with retries" in {
//      class FakeS3 extends AmazonS3Client {
//        var partNum = 0
//
//        override def uploadPart(uploadPartRequest: UploadPartRequest): UploadPartResult = {
//          partNum = partNum + 1
//          // make it fail every 5 parts
//          if (partNum % 5 == 0) {
//            throw new AmazonClientException(s"Don't feel like uploading part number ${partNum}")
//          } else {
//            super.uploadPart(uploadPartRequest)
//          }
//        }
//      }
//      val finnikyUpload = new AWSWrapper(bucketName, "", new FakeS3())
//
//      val url = getClass.getResource("/recap_res.obj.zip")
//      val file = new File(url.getFile)
//      println(s"Reading file of size: ${file.length()}")
//      val imageSource = SynchronousFileSource(file)
//      val res = imageSource
//        .runWith(finnikyUpload.multipartUploadSink("recap_res_2.obj.zip"))
//      while (!res.isTerminated) Thread.sleep(100)
//
//      // check for md5
//      val metadataRes = Await.result(aws.getObjectMetadata("recap_res_2.obj.zip"), 60 seconds)
//      assert(metadataRes.getETag == "4c412eb0b9ee1c7f628cbcf07ae9bbac-10")
//    }

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

      val url = getClass.getResource("/recap_res.obj.zip")
      val file = new File(url.getFile)
      println(s"Reading file of size: ${file.length()}")
      val imageSource = SynchronousFileSource(file)
      try {
        val res = imageSource
          .runWith(badUpload.multipartUploadSink("recap_res_3.obj.zip"))
        while (!res.isTerminated) Thread.sleep(100)

        println("did not get caught")
        assert(false)
      } catch {
        case ex: AWSException => assert(true)
        case _: Throwable => assert(false)
      }

    }

  }

}
