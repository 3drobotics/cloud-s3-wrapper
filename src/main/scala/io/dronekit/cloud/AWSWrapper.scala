package io.dronekit

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import akka.event.Logging
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.stream.scaladsl.Sink
import akka.stream.stage.{AsyncStage, PushPullStage}
import akka.util.ByteString
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

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
class AWSWrapper(awsBucket: String, awsPathPrefix: String, logger: LoggingAdapter, S3Client: AmazonS3Client = S3.client)(implicit ec: ExecutionContext) {

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
      val obj = S3Client.getObject(awsBucket, key)
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
      S3Client.putObject(awsBucket, name, dataInputStream, new ObjectMetadata())
      awsPathPrefix+name
    }
  }

  def getSignedUrl(key: String): Future[String] = {
    val expiration: java.util.Date = new DateTime().plusHours(2).toDate
    Future {
      val ret = S3Client.generatePresignedUrl(awsBucket, awsPathPrefix+key, expiration)
      ret.toString
    }
  }

  def multipartUploadTransform(key: String): AsyncStage[ByteString, Int, Option[Throwable]] = {
//    Sink.actorSubscriber(S3UploadSink.props(S3Client, awsBucket, awsPathPrefix+key))
    new S3UploadSink(S3Client, awsBucket, awsPathPrefix+key, logger)
  }

}
