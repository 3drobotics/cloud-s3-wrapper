package io.dronekit.cloud

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import akka.actor.ActorRef
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 9/17/15.
 *
 */

/**
 * Container for the actual AWS client (which can't be mocked easily)
 */
object AWSClient  {
  val s3Client = new AmazonS3Client()
}

/**
 * Wrapper object around AWS client to allow mocking
 */
class AWSWrapper(awsBucket: String, awsPathPrefix: String)(implicit ec: ExecutionContext) {

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

  def getObject(s3url: String): Future[Array[Byte]] = {
    val bucket = s3url.stripPrefix("s3://").split("/")(0)
    val key = s3url.stripPrefix("s3://").split("/").tail.mkString("/")
    getObject(bucket, key)
  }

  def getObject(bucket: String, key: String): Future[Array[Byte]] = {
    Future {
      val obj = AWSClient.s3Client.getObject(bucket, key)
      toByteArray(obj.getObjectContent)
    }
  }

  def getObjectAsInputStream(s3url: String): Future[InputStream] = {
    val bucket = s3url.stripPrefix("s3://").split("/")(0)
    val key = s3url.stripPrefix("s3://").split("/").tail.mkString("/")
    Future {AWSClient.s3Client.getObject(bucket, key).getObjectContent}
  }

  def insertIntoBucket(name: String, data: ByteString): Future[String] = {
    val dataInputStream = new ByteArrayInputStream(data.toByteBuffer.array())
    Future {
      AWSClient.s3Client.putObject(awsBucket, name, dataInputStream, new ObjectMetadata())
      awsPathPrefix+name
    }
  }

  def getSignedUrl(key: String): Future[String] = {
    val expiration: java.util.Date = new DateTime().plusHours(2).toDate
    Future {
      val ret = AWSClient.s3Client.generatePresignedUrl(awsBucket, awsPathPrefix+key, expiration)
      ret.toString
    }
  }

  def multipartUploadSink(key: String): Sink[Any, ActorRef] = {
    Sink.actorSubscriber(S3UploadSink.props(AWSClient.s3Client, awsBucket, awsPathPrefix+key))
  }
}
