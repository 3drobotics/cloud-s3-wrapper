package io.dronekit

import java.io.ByteArrayInputStream
import java.util

import akka.actor.{ActorRef, Props}
import akka.event.{LoggingAdapter, Logging}
import akka.stream.actor._
import akka.stream.stage._
import akka.util.ByteString
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
//import io.dronekit.S3UploadSink.{UploadChunk, UploadCompleted, UploadFailed, UploadResult, UploadStarted}

import scala.collection.JavaConversions._
import scala.collection._
import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.{Try, Failure, Success}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 8/18/15.
 *
 */

/**
 * An Akka Streams Sink which uploads a ByteString to S3 as a multipart upload.
 * @param s3Client The S3 client to use to connect to S3
 * @param bucket The name of the bucket to upload to
 * @param key The key to use for the upload
 */
class S3UploadSink(s3Client: AmazonS3Client, bucket: String, key: String, adapter: LoggingAdapter)(implicit ec: ExecutionContext) extends AsyncStage[ByteString, Int, Option[Throwable]] {

  val retries = 2
  sealed trait UploadState
  case object UploadStarted extends UploadState
  case class UploadFailed(ex: Throwable) extends UploadState
  case object UploadCompleted extends UploadState

  case class UploadChunk(number: Long, data: ByteString, state: UploadState = UploadStarted, etag: Option[PartETag])
  case class UploadResult(number: Long, state: UploadState, etag: Option[PartETag] = None)

  val MaxQueueSize = 10
  val MaxBufferSize = 1024 * 1024 * 10 // 10 MiB
  val chunkMap = new mutable.LongMap[UploadChunk](MaxQueueSize)
  var buffer = ByteString.empty
  var partNumber = 1
  var partEtags = List[UploadResult]()
  val uploadRequest = new InitiateMultipartUploadRequest(bucket, key)
  var multipartUpload: Option[InitiateMultipartUploadResult] = None
  try {
    multipartUpload = Some(s3Client.initiateMultipartUpload(uploadRequest))
  } catch {
    case ex: Throwable => {
      adapter.debug(s"s3Client.initiateMultipartUpload failed with ${ex}")
    }
  }

  var multipartCompleted = false
  private var onLastChunk = false

  override def onAsyncInput(event: Option[Throwable], ctx: AsyncContext[Int, Option[Throwable]]): Directive = {
    if (event.nonEmpty) {
      abortUpload()
      ctx.fail(event.get)
    } else {
      if (outstandingChunks == 0 && ctx.isFinishing) {
        try {
          completeUpload()
          ctx.finish()
        } catch {
          case ex: Throwable => {
            abortUpload()
            ctx.fail(ex)
          }
        }
      } else {
        // future completed
        ctx.ignore()
      }
    }
  }

  override def onPush(elem: ByteString, ctx: AsyncContext[Int, Option[Throwable]]): UpstreamDirective = {
    buffer ++= elem

    if (buffer.length > MaxBufferSize) {
      uploadBuffer(ctx.getAsyncCallback())
      ctx.pull()
    } else {
      // get more data
      ctx.pull()
    }
  }

  override def onPull(ctx: AsyncContext[Int, Option[Throwable]]): DownstreamDirective = {
    if (ctx.isFinishing) {
      onLastChunk = true
      uploadBuffer(ctx.getAsyncCallback())

      ctx.holdDownstream()
    } else {
      ctx.holdDownstream()
    }
  }

  override def onUpstreamFinish(ctx: AsyncContext[Int, Option[Throwable]]): TerminationDirective = {
    ctx.absorbTermination()
  }

  def setPartCompleted(partNumber: Long, etag: Option[PartETag]): Unit = {
    chunkMap(partNumber) = UploadChunk(partNumber, ByteString.empty, UploadCompleted, etag)
  }

  private def uploadPartToAmazon(buffer: ByteString, partNumber: Int, s3Client: AmazonS3Client,
                                 multipartId: String, bucket: String, key: String, retryNum: Int = 0): Future[Unit] = {
    val futureWrapper = Promise[Unit]()

    def uploadHelper(retryNumLocal: Int) {
      val uploadFuture = Future {
        val inputStream = new ByteArrayInputStream(buffer.toArray[Byte])
        val partUploadRequest = new UploadPartRequest()
          .withBucketName(bucket)
          .withKey(key)
          .withUploadId(multipartId)
          .withPartNumber(partNumber)
          .withPartSize(buffer.length)
          .withInputStream(inputStream)

        s3Client.uploadPart(partUploadRequest)
      }
      adapter.debug(s"Uploading part $partNumber")

      uploadFuture.onComplete {
        case Success(result) => {
          adapter.debug(s"Upload completed successfully for $partNumber")

          setPartCompleted(result.getPartNumber, Some(result.getPartETag))
          futureWrapper.success()
        }
        case Failure(ex) => {
          // try again
          if (retryNumLocal >= retries) {
            futureWrapper.failure(new AWSException(s"Uploading part failed for part ${partNumber} and multipartId: ${multipartId}"))
          } else {
            adapter.debug(s"Retrying upload on part ${partNumber}, on retry ${retryNumLocal}")
            uploadHelper(retryNumLocal + 1)
          }
        }
      }
    }

    uploadHelper(retryNum)

    futureWrapper.future
  }


  private def outstandingChunks: Long = {
    chunkMap.filter{case (part, chunk) => chunk.state != UploadCompleted}.toList.length
  }

  private def abortUpload() = {
    adapter.debug(s"Aborting upload to $bucket/$key with id ${multipartUpload.get.getUploadId}")
    s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, multipartUpload.get.getUploadId))
  }

  private def completeUpload() = {
    val etagList = chunkMap.toList.sortBy {case (part, chunk) => part}.map{case (part, chunk) => chunk.etag.get}
    val etagArrayList: util.ArrayList[PartETag] = new util.ArrayList[PartETag](etagList.toIndexedSeq)
    val completeRequest = new CompleteMultipartUploadRequest(bucket, key, multipartUpload.get.getUploadId, etagArrayList)
    val result = s3Client.completeMultipartUpload(completeRequest)
    multipartCompleted = true
    adapter.debug(s"Completed upload: $result")
  }

  private def uploadBuffer(asyncCb: AsyncCallback[Option[Throwable]]): Future[Unit] = {
    if (multipartUpload.isEmpty) {
      asyncCb.invoke(Some(new AWSException("Could not initiate multipart upload")))
      Future {}
    } else {
      chunkMap(partNumber) = UploadChunk(partNumber, buffer, UploadStarted, None)
      val uploadFuture = uploadPartToAmazon(buffer, partNumber, s3Client, multipartUpload.get.getUploadId, bucket, key)
      partNumber += 1
      buffer = ByteString.empty

      uploadFuture.onComplete{
        case Success(x) => asyncCb.invoke(None)
        case Failure(ex) => {
          adapter.debug(s"Upload future got failure ${ex}")
          asyncCb.invoke(Some(ex))
        }
      }

      uploadFuture
    }
  }


}
