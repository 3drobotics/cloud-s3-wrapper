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
class S3UploadSink(s3Client: AmazonS3Client, bucket: String, key: String)(implicit ec: ExecutionContext) extends AsyncStage[ByteString, Int, Unit] {
  private var allUploadsFinished = false

  val retries = 2
  var elementsToUpload = 0
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

  override def onAsyncInput(event: Unit, ctx: AsyncContext[Int, Unit]): Directive = {
    if (chunkMap.forall {case (part, chunk) => chunk.state == UploadCompleted } && ctx.isFinishing) {
      // if there are no more elements in the upload queue, finish the context
//      println(s"asyncstage .finish(), ${chunkMap}")
      completeUpload()
//      finishUpload()
      ctx.finish()
    } else {
      // future completed
      println(s"onAsyncInput not finished, ctx.isFinishing:${ctx.isFinishing}")
      ctx.ignore()
    }
  }

  override def onPush(elem: ByteString, ctx: AsyncContext[Int, Unit]): UpstreamDirective = {
//    println("onPush called")
    buffer ++= elem
//    println("context is not finishing")
//    uploadIfBufferFull()

    if (buffer.length > MaxBufferSize) {
      // upload and create future
//      ctx.push(uploadBuffer())
      elementsToUpload = elementsToUpload + 1
      println(s"add elements to upload ${elementsToUpload}")

      uploadBuffer(ctx.getAsyncCallback())
//      ctx.holdDownstream()
      ctx.pull()
    } else {
      // get more data
      ctx.pull()
    }
  }

  override def onPull(ctx: AsyncContext[Int, Unit]): DownstreamDirective = {
    if (ctx.isFinishing) {
      onLastChunk = true
//      println("onpull context finished")
      elementsToUpload = elementsToUpload + 1
      println(s"add elements to upload ${elementsToUpload}")
      uploadBuffer(ctx.getAsyncCallback())

      ctx.holdDownstream()
    } else {
//      println("on pull context not finished")
      ctx.holdDownstream()
//      ctx.push(0)
    }
  }

  override def onUpstreamFinish(ctx: AsyncContext[Int, Unit]): TerminationDirective = {
    ctx.absorbTermination()
  }

  def setPartCompleted(partNumber: Long, etag: Option[PartETag]): Unit = {
    chunkMap(partNumber) = UploadChunk(partNumber, ByteString.empty, UploadCompleted, etag)
    println(s"Completing $partNumber in chunkMap ${chunkMap(partNumber)}, ${chunkMap.size}")

    elementsToUpload = elementsToUpload - 1
    println(s"elements to upload ${elementsToUpload}")
    //    if (onLastChunk) {
//
//    }
  }

//  def finishUpload(): Unit ={
//    if () {
//      //      println(s"All chunks uploaded, stopping actor...")
//      completeUpload()
//      //      context.stop(self)
//    } else {
//      println("this shouldn't exist & is an error state");
//    }
//  }

  private def uploadPartToAmazon(buffer: ByteString, partNumber: Int, s3Client: AmazonS3Client,
                                 multipartId: String, bucket: String, key: String, retryNum: Int = 0): Future[UploadPartResult] = {
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

        s3Client.uploadPart(partUploadRequest)
      }
      println(s"Uploading part $partNumber")

      uploadFuture.onComplete {
        case Success(result) => {
          println(s"Upload completed successfully for $partNumber")
//          parent ! UploadResult(result.getPartNumber, UploadCompleted, Some(result.getPartETag))

          setPartCompleted(result.getPartNumber, Some(result.getPartETag))
        }
        case Failure(ex) => {
          println(s"Upload failed for $partNumber with exception $ex")
          // try again
          if (retryNum >= retries) {
//            parent ! UploadResult(partNumber, UploadFailed(ex))
            println("retries failed, this should error out")

          } else {
            println(s"retrying upload, on retry ${retryNum}")
            uploadPartToAmazon(buffer, partNumber, s3Client, multipartId, bucket, key, retryNum + 1)
          }
        }
      }

      uploadFuture
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
    println("starting complete upload")
    val etagList = chunkMap.toList.sortBy {case (part, chunk) => part}.map{case (part, chunk) => chunk.etag.get}
    val etagArrayList: util.ArrayList[PartETag] = new util.ArrayList[PartETag](etagList.toIndexedSeq)
    val completeRequest = new CompleteMultipartUploadRequest(bucket, key, multipartUpload.getUploadId, etagArrayList)
    val result = s3Client.completeMultipartUpload(completeRequest)
    multipartCompleted = true
    println(s"Completed upload: $result")
  }

  private def uploadBuffer(asyncCb: AsyncCallback[Unit]): Future[UploadPartResult] = {
    println(s"UploadBuffer for part $partNumber")
    chunkMap(partNumber) = UploadChunk(partNumber, buffer, UploadStarted, None)
//    val uploadRes = Future {
    val uploadFuture = uploadPartToAmazon(buffer, partNumber, s3Client, multipartUpload.getUploadId, bucket, key)
//      10
//    }
    partNumber += 1
    buffer = ByteString.empty

    uploadFuture.onComplete({x => asyncCb.invoke()})
    uploadFuture
  }

//  private def uploadIfBufferFull(): Future[Unit] = {
//    if (buffer.length > MaxBufferSize) {
//      uploadBuffer()
//    }
//  }

}
