package org.leachb.akka.mqlight.plug.points.network

import java.lang

import akka.actor._
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.util.ByteString
import com.ibm.mqlight.api

import scala.annotation.tailrec

/*
 * [[Publisher]] that generates [[ByteString]] events.  This Actor expects [[MqlightWrite]]
 * to generate events or [[MqlightClose]] to complete the stream.
 */
class NetworkChannelActorPublisher extends ActorPublisher[ByteString] with ActorLogging {

  import org.leachb.akka.mqlight.plug.points.network.NetworkChannelActorPublisher._

  var availableChunks: Vector[MqlightWrite] = Vector.empty

  override def receive: Receive = {
    case ActorPublisherMessage.Request(elements) =>
      signalOnNexts()

    case ActorPublisherMessage.Cancel =>
      log.debug("Unexpected unsubscribe")
      availableChunks.foreach(_.promise.setFailure(new Exception("Remote disconnected")))
      context.stop(self)

    case MqlightClose(promise) =>
      log.debug("Close request from client")
      promise.setSuccess(None.orNull)
      onCompleteThenStop()

    case write: MqlightWrite =>
      availableChunks :+= write
      signalOnNexts()
  }


  def awaitCancel(promise: api.Promise[Void]): Receive = {
    case ActorPublisherMessage.Cancel =>
      log.debug("Unsubscription after close all done")
      context.stop(self)
  }

  @tailrec private def signalOnNexts(): Unit =
    if (availableChunks.nonEmpty) {
      if (totalDemand > 0) {
        availableChunks match {
          case MqlightWrite(buffer, promise) +: tail =>
            onNext(buffer)
            promise.setSuccess(true)
            availableChunks = tail
          case _ =>
        }

        if (totalDemand > 0) signalOnNexts()
      }
    }
}

object NetworkChannelActorPublisher {
  case class MqlightClose(promise: api.Promise[Void])

  case class MqlightWrite(buffer: ByteString, promise: api.Promise[lang.Boolean])
}
