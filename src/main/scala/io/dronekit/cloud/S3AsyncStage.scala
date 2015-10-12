package io.dronekit

import java.io.ByteArrayInputStream
import java.util

import akka.event.{LoggingAdapter}
import akka.stream.stage._
import akka.util.ByteString
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._

import scala.collection.JavaConversions._
import scala.collection._
import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 8/18/15.
 *
 */

/**
 * An Akka AsyncStage which uploads a ByteString to S3 as a multipart upload.
 * @param s3Client The S3 client to use to connect to S3
 * @param bucket The name of the bucket to upload to
 * @param key The key to use for the upload
 * @param adapter a logger for debug messages
 *
 * This stage has a 10MB buffer for input data, as the buffer fills up, each chunk is uploaded to s3.
 * Each uploaded chunk is kept track of in the chunkMap. When all the parts in the chunkMap are uploaded
 * the stream finishes.
 */
class S3AsyncStage(s3Client: AmazonS3Client, bucket: String, key: String, adapter: LoggingAdapter)
                  (implicit ec: ExecutionContext) extends AsyncStage[ByteString, Int, Option[Throwable]] {

  val retries = 2 // number of retries for each s3 part

  sealed trait UploadState
  case object UploadStarted extends UploadState
  case class UploadFailed(ex: Throwable) extends UploadState
  case object UploadCompleted extends UploadState

  case class UploadChunk(number: Long, data: ByteString, state: UploadState = UploadStarted, etag: Option[PartETag])
  case class UploadResult(number: Long, state: UploadState, etag: Option[PartETag] = None)

  val MaxQueueSize = 10
  val MaxBufferSize = 1024 * 1024 * 10 // 10 MB
  val chunkMap = new mutable.LongMap[UploadChunk](MaxQueueSize)
  var buffer = ByteString.empty
  var partNumber = 1
  var partEtags = List[UploadResult]()
  val uploadRequest = new InitiateMultipartUploadRequest(bucket, key)
  var multipartUpload: Option[InitiateMultipartUploadResult] = None

  // wrap java s3 client in a try so we can abort the stage
  try {
    multipartUpload = Some(s3Client.initiateMultipartUpload(uploadRequest))
  } catch {
    case ex: Throwable => {
      adapter.debug(s"s3Client.initiateMultipartUpload failed with ${ex}")
    }
  }

  var multipartCompleted = false

  /**
   * Callback for when each part is done uploading
   * @param event has an error if the multipart upload failed, stage will abort
   * @param ctx async stage context
   * @return when all parts are done uploading, finishes the stage
   */
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

  /**
   * When data comes into the stage, initiate upload if the butter is full.
   * @param elem input data
   * @param ctx async context stage
   * @return pulls more data
   */
  override def onPush(elem: ByteString, ctx: AsyncContext[Int, Option[Throwable]]): UpstreamDirective = {
    buffer ++= elem

    if (buffer.length > MaxBufferSize) {
      uploadBuffer(ctx.getAsyncCallback())
    }

    // get more data
    ctx.pull()
  }

  /**
   * When downstream pulls for data, do nothing. If the stream is done, do the final upload buffer
   * @param ctx
   * @return always holds the downstream
   */
  override def onPull(ctx: AsyncContext[Int, Option[Throwable]]): DownstreamDirective = {
    if (ctx.isFinishing) {
      uploadBuffer(ctx.getAsyncCallback())
    }
    ctx.holdDownstream()
  }

  /**
   * override the termination because upstream will finish reading data before we are done uploading it to s3
   * @param ctx
   * @return
   */
  override def onUpstreamFinish(ctx: AsyncContext[Int, Option[Throwable]]): TerminationDirective = {
    ctx.absorbTermination()
  }

  /**
   * Set a part in the chunkmap as complete
   * @param partNumber number of the chunk
   * @param etag etag of the chunk
   */
  def setPartCompleted(partNumber: Long, etag: Option[PartETag]): Unit = {
    chunkMap(partNumber) = UploadChunk(partNumber, ByteString.empty, UploadCompleted, etag)
  }

  /**
   * Upload a buffer of data into s3, retries 2 times by default
   * @param buffer data to upload
   * @param partNumber the chunk for the chunkMap
   * @param multipartId id of the multipart upload
   * @param bucket s3 bucket name
   * @param key name of file
   * @return the etag of the part upload
   */
  private def uploadPartToAmazon(buffer: ByteString, partNumber: Int,
                                 multipartId: String, bucket: String, key: String): Future[Unit] = {
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

    uploadHelper(0)

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

  /**
   * upload part to s3 and return the result to the async stage callback
   * @param asyncCb async stage callback
   */
  private def uploadBuffer(asyncCb: AsyncCallback[Option[Throwable]]): Future[Unit] = {
    if (multipartUpload.isEmpty) {
      asyncCb.invoke(Some(new AWSException("Could not initiate multipart upload")))
      Future {}
    } else {
      chunkMap(partNumber) = UploadChunk(partNumber, buffer, UploadStarted, None)
      val uploadFuture = uploadPartToAmazon(buffer, partNumber, multipartUpload.get.getUploadId, bucket, key)
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
