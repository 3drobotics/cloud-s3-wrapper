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
import scala.util.{Failure, Success}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 8/18/15.
 *
 */


//object S3UploadSink {
//
//
//}

class S3UploadAsync()(implicit ec: ExecutionContext) extends AsyncStage[Future[Int], Int, Int] {
  var elementsToUpload = 0

  override def onAsyncInput(event: Int, ctx: AsyncContext[Int, Int]): Directive = {
    elementsToUpload = elementsToUpload - 1

    if (elementsToUpload == 0 && ctx.isFinishing) {
      // if there are no more elements in the upload queue, finish the context
      println("asyncstage .finish()")
      ctx.finish()
    } else {
      // future completed
      println(s"onAsyncInput not finished, ctx.isFinishing:${ctx.isFinishing}")
      ctx.push(0)
    }
  }

  override def onPush(elem: Future[Int], ctx: AsyncContext[Int, Int]): SyncDirective = {
    println("AsyncStage onPush called")
    elementsToUpload = elementsToUpload + 1
    elem.onSuccess { case x => {
      println("future completed, invoking...")
      ctx.getAsyncCallback().invoke(x)
    } }

    // abort on failure
    elem.onFailure {case ex => {
      println("future failed, should abort")
      ctx.fail(new AWSException("something bad happened"))
    }}

    ctx.push(0)
  }

  override def onPull(ctx: AsyncContext[Int, Int]): SyncDirective = {
    println("AsyncStage onPull called")
    ctx.pull()
  }

  override def onUpstreamFinish(ctx: AsyncContext[Int, Int]): TerminationDirective = {
    println("AsyncStage overriding onUpstreamFinish")
    ctx.absorbTermination()
  }
}

/**
 * An Akka Streams Sink which uploads a ByteString to S3 as a multipart upload.
 * @param s3Client The S3 client to use to connect to S3
 * @param bucket The name of the bucket to upload to
 * @param key The key to use for the upload
 */
class S3UploadSink(s3Client: AmazonS3Client, bucket: String, key: String)(implicit ec: ExecutionContext) extends PushPullStage[ByteString, Future[Int]] {
  private var allUploadsFinished = false

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
//  var upstreamComplete = false
//  var requested = 0
  var partEtags = List[UploadResult]()
  val uploadRequest = new InitiateMultipartUploadRequest(bucket, key)
  val multipartUpload = s3Client.initiateMultipartUpload(uploadRequest)
  var multipartCompleted = false
  private var onLastChunk = false

  override def onPush(elem: ByteString, ctx: Context[Future[Int]]): SyncDirective = {
    println("onPush called")
    buffer ++= elem
    println("context is not finishing")
//    uploadIfBufferFull()

    if (buffer.length > MaxBufferSize) {
      ctx.push(uploadBuffer())
    } else {
      ctx.push(Future{0}) // bogus number for now
    }
  }

  override def onPull(ctx: Context[Future[Int]]): SyncDirective = {
    if (ctx.isFinishing) {
      onLastChunk = true
      println("onpull context finished")
      uploadBuffer()
      completeUpload()
      ctx.finish()
    } else {
      println("on pull context not finished")
      ctx.pull()
    }
  }

  override def onUpstreamFinish(ctx: Context[Future[Int]]): TerminationDirective = {
    ctx.absorbTermination()
  }

  def setPartCompleted(partNumber: Long, etag: Option[PartETag]): Unit = {
    println(s"Completing $partNumber in chunkMap")
    chunkMap(partNumber) = UploadChunk(partNumber, ByteString.empty, UploadCompleted, etag)
    if (onLastChunk) {
      if (chunkMap.forall {case (part, chunk) => chunk.state == UploadCompleted }) {
        //      println(s"All chunks uploaded, stopping actor...")
        completeUpload()
        //      context.stop(self)
      } else {
        println(" this shouldn't exist & is an error state");
      }
    }
  }

  private def uploadPartToAmazon(buffer: ByteString, partNumber: Int, s3Client: AmazonS3Client,
                                 multipartId: String, bucket: String, key: String, retryNum: Int = 0): Unit = {
    // TODO: How to get logger in this method?
      val inputStream = new ByteArrayInputStream(buffer.toArray[Byte])
      val partUploadRequest = new UploadPartRequest()
        .withBucketName(bucket)
        .withKey(key)
        .withUploadId(multipartId)
        .withPartNumber(partNumber)
        .withPartSize(buffer.length)
        .withInputStream(inputStream)

      println(s"Uploading part $partNumber")
      try {
        val result = s3Client.uploadPart(partUploadRequest)
        setPartCompleted(result.getPartNumber, Some(result.getPartETag))

      } catch {
        case ex: Throwable => {
          println(s"Upload failed for $partNumber with exception $ex")
          // try again
          if (retryNum >= retries) {
            //          setPartCompleted(UploadResult(partNumber, UploadFailed(ex)))

            // signal failure here
            println("retries failed, this should error out")
          } else {
            println(s"retrying upload, on retry ${retryNum}")
            uploadPartToAmazon(buffer, partNumber, s3Client, multipartId, bucket, key, retryNum + 1)
          }
        }
      }
    }



  private def outstandingChunks: Long = {
    chunkMap.filter{case (part, chunk) => chunk.state != UploadCompleted}.toList.length
  }

//  private def signalDemand(ctx: Context[Int]): Unit = {
//    val freeBuffers = MaxQueueSize - outstandingChunks
//    val availableDemand = MaxQueueSize - requested
//    if (freeBuffers > 0 && !upstreamComplete) {
//      request(availableDemand)
//      requested += availableDemand
//    }
//  }
//
//  signalDemand()

  private def abortUpload() = {
    println(s"Aborting upload to $bucket/$key with id ${multipartUpload.getUploadId}")
    s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, multipartUpload.getUploadId))
//    context.stop(self)
  }

  private def completeUpload() = {
    val etagList = chunkMap.toList.sortBy {case (part, chunk) => part}.map{case (part, chunk) => chunk.etag.get}
    val etagArrayList: util.ArrayList[PartETag] = new util.ArrayList[PartETag](etagList.toIndexedSeq)
    val completeRequest = new CompleteMultipartUploadRequest(bucket, key, multipartUpload.getUploadId, etagArrayList)
    val result = s3Client.completeMultipartUpload(completeRequest)
    multipartCompleted = true
    println(s"Completed upload: $result")
  }

  private def uploadBuffer(): Future[Int] = {
    println(s"UploadBuffer for part $partNumber")
    chunkMap(partNumber) = UploadChunk(partNumber, buffer, UploadStarted, None)
    val uploadRes = Future {
      uploadPartToAmazon(buffer, partNumber, s3Client, multipartUpload.getUploadId, bucket, key)
      10
    }
    partNumber += 1
    buffer = ByteString.empty

    uploadRes
  }

//  private def uploadIfBufferFull(): Future[Unit] = {
//    if (buffer.length > MaxBufferSize) {
//      uploadBuffer()
//    }
//  }

}
