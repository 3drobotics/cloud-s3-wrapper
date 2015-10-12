package io.dronekit

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage.AsyncStage
import akka.util.ByteString
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import org.joda.time.DateTime

import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 9/17/15.
 *
 */

class AWSException(msg: String) extends RuntimeException

/**
 * Container for the actual AWS client (which can't be mocked easily)
 */
object S3  {
  val client = new AmazonS3Client()
}

/**
 * Wrapper object around AWS client to allow mocking
 */
class AWSWrapper(val awsBucket: String, val awsPathPrefix: String, S3Client: AmazonS3Client = S3.client)(implicit ec: ExecutionContext) {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val adapter: LoggingAdapter = Logging(system, "AWSWrapper")

  private def toByteArray(src: InputStream) = {
    val buffer = new ByteArrayOutputStream()
    var nRead = 0
    val data = new Array[Byte](16384)
    while (nRead != -1) {
      nRead = src.read(data, 0, data.length)
      if (nRead > 0)
        buffer.write(data, 0, nRead)
    }
    buffer.flush()
    buffer.toByteArray
  }

  def getObjectS3URL(s3url: String): Future[Array[Byte]] = {
    val bucket = s3url.stripPrefix("s3://").split("/")(0)
    val key = s3url.stripPrefix("s3://").split("/").tail.mkString("/")
    getObject(bucket, key)
  }

  private def getObject(bucket: String, key: String): Future[Array[Byte]] = {
    Future {
      val obj = S3Client.getObject(bucket, key)
      toByteArray(obj.getObjectContent)
    }
  }

  def getObject(key: String): Future[Array[Byte]] = {
    Future {
      val obj = S3Client.getObject(awsBucket, awsPathPrefix+key)
      toByteArray(obj.getObjectContent)
    }
  }

  def getObjectMetadata(key:String): Future[ObjectMetadata] = {
    Future {
      S3Client.getObjectMetadata(awsBucket, awsPathPrefix+key)
    }
  }

  def getObjectAsInputStream(s3url: String): Future[InputStream] = {
    val bucket = s3url.stripPrefix("s3://").split("/")(0)
    val key = s3url.stripPrefix("s3://").split("/").tail.mkString("/")
    Future {S3Client.getObject(bucket, key).getObjectContent}
  }

  def insertIntoBucket(name: String, data: ByteString): Future[String] = {
    val dataInputStream = new ByteArrayInputStream(data.toByteBuffer.array())
    Future {
      S3Client.putObject(awsBucket, awsPathPrefix+name, dataInputStream, new ObjectMetadata())
      S3Client.getUrl(awsBucket, awsPathPrefix+name).toString
    }
  }

  def getSignedUrl(key: String, expiration: java.util.Date = new DateTime().plusHours(2).toDate): Future[String] = {
    Future {
      val ret = S3Client.generatePresignedUrl(awsBucket, awsPathPrefix+key, expiration)
      ret.toString
    }
  }

  def streamInsertIntoBucket(dataSource: Source[ByteString, Any], key: String): Future[String] = {
    val p = Promise[String]()
    val streamRes = dataSource.transform(()=> multipartUploadTransform(key)).runWith(Sink.ignore)
    streamRes.onComplete{
      case Success(x) => p.success(S3Client.getUrl(awsBucket, awsPathPrefix+key).toString)
      case Failure(ex) => p.failure(ex)
    }
    p.future
  }

  def multipartUploadTransform(key: String): AsyncStage[ByteString, Int, Option[Throwable]] = {
    new S3AsyncStage(S3Client, awsBucket, awsPathPrefix+key, adapter)
  }

}
