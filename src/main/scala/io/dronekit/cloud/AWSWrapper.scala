package io.dronekit.cloud

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.stream.{SinkShape, FlowShape, ActorMaterializer}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.stage.{GraphStageWithMaterializedValue, GraphStage}
import akka.util.ByteString
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{CompleteMultipartUploadResult, UploadPartResult, AmazonS3Exception, ObjectMetadata}
import org.joda.time.DateTime

import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 9/17/15.
 *
 */

class AWSException(msg: String) extends RuntimeException

/**
 * case class representing an internal S3 location
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
class AWSWrapper(val awsBucket: String, S3Client: AmazonS3Client = S3.client)
                (implicit ec: ExecutionContext, logger: LoggingAdapter) {
  require(ec != null, "Execution context was null!")

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

  /**
   * Get an object from S3 as a byte array
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
   * @param s3url The url of the form s3://bucketname/key
   * @return an InputStream for the found object
   */
  def getObjectAsInputStream(s3url: S3URL): Future[InputStream] = {
    Future {
      try
        S3Client.getObject(s3url.bucket, s3url.key).getObjectContent
      catch {
        case ex: AmazonS3Exception =>
          logger.error(ex, s"Failed to get $s3url")
          throw ex
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
   * @param key The location in the bucket to save the object
   * @param data The data to insert
   * @return The S3 URL (of the form s3://bucketname/key)
   */
  def insertIntoBucket(key: String, data: ByteString): Future[S3URL] = {
    val dataInputStream = new ByteArrayInputStream(data.toByteBuffer.array())
    Future {
      val metadata = new ObjectMetadata()
      metadata.setContentLength(data.length.toLong)
      S3Client.putObject(awsBucket, key, dataInputStream, metadata)
      S3URL(awsBucket, key)
    }
  }

  /**
   * Return a signed URL for the object in the configured bucket with key
   * @param s3url The location in S3 of the object
   * @return A HTTP url for the object. Will time out!
   */
  def getSignedUrl(s3url: S3URL): Future[String] = {
    val expiration: java.util.Date = new DateTime().plusHours(2).toDate
    Future {
      val ret = S3Client.generatePresignedUrl(s3url.bucket, s3url.key, expiration)
      ret.toString
    }
  }

  def streamInsertIntoBucket(dataSource: Source[ByteString, Any], key: String): Future[S3URL] = {
    val p = Promise[S3URL]()
    val s3url = S3URL(awsBucket, key)
    val streamRes = dataSource.via(multipartUploadTransform(s3url)).runWith(Sink.ignore)
    streamRes.onComplete{
      case Success(x) => p.success(s3url)
      case Failure(ex) => p.failure(ex)
    }
    p.future
  }

  def multipartUploadTransform(s3url: S3URL): Flow[ByteString, UploadPartResult, Future[CompleteMultipartUploadResult]] = {
    Flow.fromGraph(new S3UploadFlow(S3Client, s3url.bucket, s3url.key, adapter))
  }

}
