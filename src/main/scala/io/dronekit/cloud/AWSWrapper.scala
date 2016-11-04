package io.dronekit.cloud

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.time.{ZoneId, ZonedDateTime}
import java.util.Date

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.model.{AmazonS3Exception, CompleteMultipartUploadResult, GeneratePresignedUrlRequest, ObjectMetadata, ResponseHeaderOverrides, UploadPartResult}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Created by Jason Martens <jason.martens@3drobotics.com> on 9/17/15.
  *
  */

class AWSException(msg: String) extends RuntimeException(msg)

/**
  * case class representing an internal S3 location
  *
  * @param bucket The name of the bucket
  * @param key The name of the key
  */
case class S3URL(bucket: String, key: String) {
  override def toString = s"s3://$bucket/$key"
}

/**
  * Container for the actual AWS client (which can't be mocked easily)
  */
object S3  {
  val client = new AmazonS3Client()
}

/**
  * Wrapper object around AWS client to allow mocking
  */
class AWSWrapper(S3Client: AmazonS3Client = S3.client)
                (implicit ec: ExecutionContext) {
  require(ec != null, "Execution context was null!")
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  val logger: Logger = Logger(LoggerFactory.getLogger(getClass))

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

  /**
    * Get an object from S3 as a byte array
    *
    * @param s3url The location of the object in S3
    * @return A byte array
    */
  def getObject(s3url: S3URL): Future[Array[Byte]] = {
    Future {
      val obj = S3Client.getObject(s3url.bucket, s3url.key)
      toByteArray(obj.getObjectContent)
    }
  }

  /**
    * Return an input stream to an object located in S3
    *
    * @param s3url The url of the form s3://bucketname/key
    * @return an InputStream for the found object
    */
  def getObjectAsInputStream(s3url: S3URL): Future[InputStream] = {
    Future {
      try
        S3Client.getObject(s3url.bucket, s3url.key).getObjectContent
      catch {
        case ex: AmazonS3Exception =>
          logger.error(s"Failed to get $s3url: $ex")
          throw ex
      }
    }
  }

  /**
    * Get an object via HTTP directly, rather than through the Java API
    *
    * @param s3url The URL of the object to get
    * @return A ByteString of the data
    */
  def getObjectAsByteString(s3url: S3URL): Future[ByteString] = {
    getSignedUrl(s3url).flatMap { url =>
      Http().singleRequest(HttpRequest(HttpMethods.GET, uri = url)).flatMap { resp =>
        resp.entity.dataBytes.runFold[ByteString](ByteString())(_ ++ _)
      }
    }
  }

  def getObjectMetadata(s3url: S3URL): Future[ObjectMetadata] = {
    Future {
      S3Client.getObjectMetadata(s3url.bucket, s3url.key)
    }
  }

  /**
    * Insert an object into the bucket
    *
    * @param s3url The location in the bucket to save the object
    * @param data The data to insert
    * @return The S3 URL (of the form s3://bucketname/key)
    */
  def insertIntoBucket(s3url: S3URL, data: ByteString): Future[S3URL] = {
    val dataInputStream = new ByteArrayInputStream(data.toByteBuffer.array())
    Future {
      val metadata = new ObjectMetadata()
      metadata.setContentLength(data.length.toLong)
      S3Client.putObject(s3url.bucket, s3url.key, dataInputStream, metadata)
      S3URL(s3url.bucket, s3url.key)
    }
  }

  def deleteObject(s3url: S3URL): Future[Unit] = {
    Future {
      S3Client.deleteObject(s3url.bucket, s3url.key)
    }
  }

  /**
  * Wrap sign url in a future for legacy composition
  */
  def getSignedUrl(s3url: S3URL, expiry: ZonedDateTime = ZonedDateTime.now(ZoneId.of("UTC")).plusDays(7), filename: Option[String] = None): Future[String] = {
    Future {
      signUrl(s3url, expiry, filename)
    }
  }

  /**
    * Return a signed URL for the object in the configured bucket with key
    * max 7 day expiration date: http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
    *
    * @param s3url The location in S3 of the object
    * @return A HTTP url for the object. Will time out!
    */
  def signUrl(s3url: S3URL, expiry: ZonedDateTime = ZonedDateTime.now(ZoneId.of("UTC")).plusDays(7), filename: Option[String] = None): String = {
    filename match {
      case None => S3Client.generatePresignedUrl(s3url.bucket, s3url.key, Date.from(expiry.toInstant)).toString
      case Some(name) => {
        val request: GeneratePresignedUrlRequest = new GeneratePresignedUrlRequest(s3url.bucket, s3url.key, HttpMethod.GET)
        val headerOverrides: ResponseHeaderOverrides = new ResponseHeaderOverrides()
        headerOverrides.setContentDisposition(s"attachment; filename=$name")
        request.withResponseHeaders(headerOverrides).withExpiration(Date.from(expiry.toInstant))
        S3Client.generatePresignedUrl(request).toString
      }
    }
  }

  def streamUpload(s3url: S3URL): Sink[ByteString, Future[CompleteMultipartUploadResult]] = {
    multipartUploadTransform(s3url).fold(Seq.empty[UploadPartResult])(_ :+ _).to(Sink.head)
  }

  def streamInsertIntoBucket(dataSource: Source[ByteString, Any], s3url: S3URL): Future[S3URL] = {
    dataSource
      .via(multipartUploadTransform(s3url))
      .runWith(Sink.ignore)
      .map( _ => s3url)
  }

  def multipartUploadTransform(s3url: S3URL): Flow[ByteString, UploadPartResult, Future[CompleteMultipartUploadResult]] = {
    Flow.fromGraph(new S3UploadFlow(S3Client, s3url.bucket, s3url.key, logger))
  }

}
