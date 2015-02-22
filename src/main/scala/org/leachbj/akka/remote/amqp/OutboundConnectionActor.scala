/*
 * Copyright 2003-2015 Monitise Group Limited. All Rights Reserved.
 *
 * Save to the extent permitted by law, you may not use, copy, modify,
 * distribute or create derivative works of this material or any part
 * of it without the prior written consent of Monitise Group Limited.
 * Any reproduction of this material must contain this notice.
 */
package org.leachbj.akka.remote.amqp

import akka.actor._
import akka.remote.transport.AssociationHandle
import akka.remote.transport.AssociationHandle.{Disassociated, HandleEventListener, InboundPayload}
import akka.util.ByteString
import com.ibm.mqlight.api.{BytesDelivery, StringDelivery}

import scala.concurrent.Promise

class OutboundConnectionActor(localAddr: Address, remoteAddr: Address, promise: Promise[AssociationHandle]) extends Actor with ActorLogging {

  import context.dispatcher
  import org.leachbj.akka.remote.amqp.ActorCompletionListener._
  import org.leachbj.akka.remote.amqp.ActorDestinationListener._
  import org.leachbj.akka.remote.amqp.AmqpTransport._
  import org.leachbj.akka.remote.amqp.MqLightClient._
  import org.leachbj.akka.remote.amqp.OutboundConnectionActor._

  import scala.concurrent.duration._

  val localTopic = s"${addressName(localAddr)}/${addressName(remoteAddr)}"
  val remoteTopic = s"${addressName(remoteAddr)}/${addressName(localAddr)}"

  val settings = new AmqpTransportSettings(context.system.settings.config.getConfig("akka.remote.amqp"))

  override def preStart(): Unit = {
    val client = context.system.actorOf(MqLightClient.props(Some(self)))
    client ! MqLightStart(None, settings.BrokerUrl, settings.BrokerUser, settings.BrokerPassword)
    context.become(waitForStart(client, sender()))
  }

  override def receive: Receive = Actor.emptyBehavior

  def waitForStart(client: ActorRef, actor: ActorRef): Receive = {
    case MqLightStarted(clientId) =>
      client ! MqLightSubscribe(s"$localTopic/client/+")
      context.become(waitForSubscribed(client))
    //    case Failure(e) =>
    //      promise.failure(e)
    //      context.stop(self)
  }

  def waitForSubscribed(client: ActorRef): Receive = {
    case CompletionSuccess =>
      sendConnect(client)
      val retry = context.system.scheduler.schedule(settings.ConnectRetry, settings.ConnectRetry, self, RetryConnect)
      context.become(unconnected(client, retry))
    case CompletionError(e) =>
      promise.failure(e)
      context.stop(self)
  }

  def unconnected(client: ActorRef, retry: Cancellable): Receive = {
    case Message(delivery: StringDelivery) if delivery.getData == "synack" =>
      retry.cancel()
      log.debug("{} received synack from {}", localAddr, remoteAddr)

      val handle = new AmqpAssociationHandle(localAddr, remoteAddr, client, s"$remoteTopic/server", self)
      handle.readHandlerPromise.future.onSuccess {
        case listener: HandleEventListener =>
          self ! ReadHandleSuccess(listener)
      }

      promise.success(handle)
      context.become(waitForReader(client, handle))
    case RetryConnect =>
      log.debug("{} retry connect to {}", localAddr, remoteAddr)
      sendConnect(client)
  }

  def waitForReader(client: ActorRef, handle: AmqpAssociationHandle): Receive = {
    case ReadHandleSuccess(listener) =>
      log.debug("{} read handle ready {}", localAddr, remoteAddr)
      context.become(connected(client, handle, listener))
  }

  def connected(client: ActorRef, handle: AmqpAssociationHandle, listener: HandleEventListener): Receive = {
    case Message(bytes: BytesDelivery) =>
      listener.notify(InboundPayload(ByteString.fromByteBuffer(bytes.getData)))
    case Message(delivery: StringDelivery) if delivery.getData == "disassociate" =>
      listener.notify(Disassociated(AssociationHandle.Shutdown))
      context.become(disconnecting(client))
    case MqLightDraining =>
      handle.available = false
    case MqLightDrained =>
      handle.available = true
    case AmqpConnectionDisassociate =>
      log.debug("{} disassociate", localAddr)
      client ! MqLightSendString(s"$remoteTopic/client/disassociate", "disassociate")
      val timeout = context.system.scheduler.scheduleOnce(500.milliseconds, self, DisconnectTimeout)
      context.become(startedDisconnecting(client, timeout))
  }

  def startedDisconnecting(client: ActorRef, timeout: Cancellable): Receive = {
    case CompletionSuccess =>
    case CompletionError(e) =>
    case Message(delivery: StringDelivery) if delivery.getData == "disassociate" =>
      timeout.cancel()
      context.become(disconnecting(client))
    case DisconnectTimeout =>
      log.debug("{} timed out out waiting for disconnect", localAddr)
      context.become(disconnecting(client))
  }

  def disconnecting(client: ActorRef): Receive = {
    client ! MqLightUnSubscribe(s"$localTopic/client/+")

    {
      case CompletionSuccess =>
        log.debug("{} unsubscribed from {}", localAddr, remoteAddr)
        context.stop(self)
      case CompletionError(e) =>
        log.error(e, "{} disconnect/unsubscribe from {} error", localAddr, remoteAddr)
        context.stop(self)
    }
  }

  private[this] def sendConnect(client: ActorRef) = {
    log.debug("{} sending connect request to {}", localAddr, remoteAddr)
    client ! MqLightSendString(s"$remoteTopic/connect", "connect")
  }
}

object OutboundConnectionActor {
  def props(localAddr: Address, remoteAddr: Address, promise: Promise[AssociationHandle]) =
    Props(classOf[OutboundConnectionActor], localAddr, remoteAddr, promise)

  case object RetryConnect

  case class ReadHandleSuccess(listener: HandleEventListener)

  case object DisconnectTimeout

}

