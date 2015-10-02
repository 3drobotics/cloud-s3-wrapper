package io.dronekit.cloud

import java.io.ByteArrayInputStream
import java.util

import akka.actor.{ActorRef, Props}
import akka.event.Logging
import akka.stream.actor._
import akka.util.ByteString
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import io.dronekit.cloud.S3UploadSink.{UploadChunk, UploadCompleted, UploadFailed, UploadResult, UploadStarted}

import scala.collection.JavaConversions._
import scala.collection._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 8/18/15.
 *
 */


object S3UploadSink {

  sealed trait UploadState
  case object UploadStarted extends UploadState
  case class UploadFailed(ex: Throwable) extends UploadState
  case object UploadCompleted extends UploadState

  case class UploadChunk(number: Long, data: ByteString, state: UploadState = UploadStarted, etag: Option[PartETag])
  case class UploadResult(number: Long, state: UploadState, etag: Option[PartETag] = None)

  def props(s3Client: AmazonS3Client, bucket: String, key: String): Props =
    Props(new S3UploadSink(s3Client, bucket, key))

  private def uploadPartToAmazon(parent: ActorRef, buffer: ByteString, partNumber: Int, s3Client: AmazonS3Client,
                         multipartId: String, bucket: String, key: String)(implicit ec: ExecutionContext): Unit = {
    // TODO: How to get logger in this method?
    val uploadFuture = Future {
      val inputStream = new ByteArrayInputStream(buffer.toArray[Byte])
      val partUploadRequest = new UploadPartRequest()
        .withBucketName(bucket)
        .withKey(key)
        .withUploadId(multipartId)
        .withPartNumber(partNumber)
        .withPartSize(buffer.length)
        .withInputStream(inputStream)
      println(s"Uploading part $partNumber")
      s3Client.uploadPart(partUploadRequest)
    }
    uploadFuture.onComplete {
      case Success(result) => {
        println(s"Upload completed successfully for $partNumber")
        parent ! UploadResult(result.getPartNumber, UploadCompleted, Some(result.getPartETag))
      }
      case Failure(ex) => {
        println(s"Upload failed for $partNumber with exception $ex")
        parent ! UploadResult(partNumber, UploadFailed(ex))
      }
    }
  }
}

/**
 * An Akka Streams Sink which uploads a ByteString to S3 as a multipart upload.
 * @param s3Client The S3 client to use to connect to S3
 * @param bucket The name of the bucket to upload to
 * @param key The key to use for the upload
 */
class S3UploadSink(s3Client: AmazonS3Client, bucket: String, key: String) extends ActorSubscriber {
  import ActorSubscriberMessage._
  implicit val ec: ExecutionContext = context.system.dispatcher
  val logger = Logging(context.system, getClass)

  override protected def requestStrategy: RequestStrategy = ZeroRequestStrategy

  val MaxQueueSize = 10
  val MaxBufferSize = 1024 * 1024 * 10 // 10 MiB
  val chunkMap = new mutable.LongMap[UploadChunk](MaxQueueSize)
  var buffer = ByteString.empty
  var partNumber = 1
  var upstreamComplete = false
  var requested = 0
  var partEtags = List[UploadResult]()
  val uploadRequest = new InitiateMultipartUploadRequest(bucket, key)
  val multipartUpload = s3Client.initiateMultipartUpload(uploadRequest)
  var multipartCompleted = false

  private def outstandingChunks: Long = {
    chunkMap.filter{case (part, chunk) => chunk.state != UploadCompleted}.toList.length
  }

  private def signalDemand(): Unit = {
    val freeBuffers = MaxQueueSize - outstandingChunks
    val availableDemand = 10 - requested
    if (freeBuffers > 0 && !upstreamComplete) {
      request(availableDemand)
      requested += availableDemand
    }
  }

  signalDemand()

  private def abortUpload() = {
    logger.warning(s"Aborting upload to $bucket/$key with id ${multipartUpload.getUploadId}")
    s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, multipartUpload.getUploadId))
    context.stop(self)
  }

  private def completeUpload() = {
    val etagList = chunkMap.toList.sortBy {case (part, chunk) => part}.map{case (part, chunk) => chunk.etag.get}
    val etagArrayList: util.ArrayList[PartETag] = new util.ArrayList[PartETag](etagList.toIndexedSeq)
    val completeRequest = new CompleteMultipartUploadRequest(bucket, key, multipartUpload.getUploadId, etagArrayList)
    val result = s3Client.completeMultipartUpload(completeRequest)
    multipartCompleted = true
    logger.info(s"Completed upload: $result")
  }

  private def uploadIfBufferFull(): Unit = {
    if (buffer.length > MaxBufferSize)
      uploadBuffer()
  }

  private def uploadBuffer(): Unit = {
    logger.debug(s"UploadBuffer for part $partNumber")
    chunkMap(partNumber) = UploadChunk(partNumber, buffer, UploadStarted, None)
    S3UploadSink.uploadPartToAmazon(self, buffer, partNumber, s3Client, multipartUpload.getUploadId, bucket, key)
    partNumber += 1
    buffer = ByteString.empty
  }

  def setPartCompleted(partNumber: Long, etag: Option[PartETag]): Unit = {
    logger.debug(s"Completing $partNumber in chunkMap")
    chunkMap(partNumber) = UploadChunk(partNumber, ByteString.empty, UploadCompleted, etag)
    if (upstreamComplete) {
      if (chunkMap.forall {case (part, chunk) => chunk.state == UploadCompleted }) {
      logger.info(s"All chunks uploaded, stopping actor...")
      completeUpload()
      context.stop(self)
      }
    }
    else signalDemand()
  }

  override def receive: Receive = {
    case OnNext(msg: ByteString) => {
      buffer ++= msg
      requested -= 1
      uploadIfBufferFull()
      signalDemand()
    }
    case OnError(err) => {
      logger.warning(s"onError: $err. Aborting upload")
      abortUpload()
    }
    case OnComplete => {
      logger.info("OnComplete, waiting for upload parts to finish")
      upstreamComplete = true
      uploadBuffer()
    }
    case UploadResult(number, status, etag) => status match {
      case UploadStarted =>
        logger.error(s"ERROR, should not get an upload started message. number: $number, etag: $etag")
      case UploadFailed(ex) => {
        logger.warning(s"Upload of part $number failed with exception: $ex")
        abortUpload()
      }
      case UploadCompleted => {
        logger.info(s"Upload of part $number completed. Demand: $requested")
        setPartCompleted(number, etag)
      }
    }
  }
}
