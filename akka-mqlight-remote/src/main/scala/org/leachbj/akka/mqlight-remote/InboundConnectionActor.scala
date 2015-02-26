/**
 * Copyright (c) 2015 Bernard Leach
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.leachbj.akka.mqlight.remote

import akka.actor._
import akka.remote.transport.AssociationHandle
import akka.remote.transport.AssociationHandle.{Disassociated, HandleEventListener, InboundPayload}
import akka.remote.transport.Transport.{AssociationEventListener, InboundAssociation}
import akka.util.ByteString
import com.ibm.mqlight.api.{BytesDelivery, MalformedDelivery, StringDelivery}
import org.leachbj.akka.mqlight.client.MqLightClient

class InboundConnectionActor(localAddr: Address, remoteAddr: Address, eventListener: AssociationEventListener) extends Actor with Stash with ActorLogging {

  import context.dispatcher
  import org.leachbj.akka.mqlight.client.ActorCompletionListener._
  import org.leachbj.akka.mqlight.client.ActorDestinationListener._
  import org.leachbj.akka.mqlight.remote.AmqpTransport._
  import org.leachbj.akka.mqlight.remote.InboundConnectionActor._
  import org.leachbj.akka.mqlight.client.MqLightClient._

  import scala.concurrent.duration._

  val localTopic = s"${addressName(localAddr)}/${addressName(remoteAddr)}"
  val remoteTopic = s"${addressName(remoteAddr)}/${addressName(localAddr)}"

  val settings = new AmqpTransportSettings(context.system.settings.config.getConfig("akka.remote.amqp"))

  override def preStart(): Unit = {
    val client = context.system.actorOf(MqLightClient.props(Some(self)))
    client ! MqLightStart(None, settings.BrokerUrl, settings.BrokerUser, settings.BrokerPassword)
    context.become(waitForStart(client))
  }

  override def receive: Receive = Actor.emptyBehavior

  def waitForStart(client: ActorRef): Receive = {
    case MqLightStarted(clientId) =>
      log.debug("{} subscribing to listen address", localAddr)
      client ! MqLightSubscribe(s"$localTopic/server/+")
      context.become(waitForSubscribed(client))
  }

  def waitForSubscribed(client: ActorRef): Receive = {
    case CompletionSuccess =>
      sendSynAck(client)

      val handle = new AmqpAssociationHandle(localAddr, remoteAddr, client, s"$remoteTopic/client", self)
      eventListener.notify(InboundAssociation(handle))

      handle.readHandlerPromise.future.onSuccess {
        case listener: HandleEventListener =>
          self ! ListenAssociated(listener)
      }

      context.become(awaitAssociation(client, handle))
    case CompletionError(e) =>
      throw e
  }

  def awaitAssociation(client: ActorRef, handle: AmqpAssociationHandle): Receive = {
    case ListenAssociated(listener) =>
      log.debug("{} listen associated", localAddr)
      unstashAll()
      context.become(associated(client, handle, listener))
    case _: Message =>
      log.debug("{} message received before associated", localAddr)
      stash()
    case Malformed(delivery: MalformedDelivery) =>
      log.debug("{} malformed message received before associated", localAddr)
    case Unsubscribed(topicPattern, share, error) =>
      log.error(error, "{} unsubscribed from {}", localAddr, topicPattern)
      throw error
  }

  def associated(client: ActorRef, handle: AmqpAssociationHandle, listener: HandleEventListener): Receive = {
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
      log.debug("{} timedout out waiting for disconnect", localAddr)
      context.become(disconnecting(client))
  }

  def disconnecting(client: ActorRef): Receive = {
    client ! MqLightUnSubscribe(s"$localTopic/server/+")

    {
      case CompletionSuccess =>
        log.debug("{} unsubscribed from {}", localAddr, remoteAddr)
        context.stop(client)
        context.stop(self)
      case CompletionError(e) =>
        log.error(e, "{} disconnect/unsubscribe from {} error", localAddr, remoteAddr)
        context.stop(client)
        context.stop(self)
    }
  }

  def sendSynAck(client: ActorRef) = {
    log.debug("{} sending synack to {}", localAddr, remoteAddr)
    client ! MqLightSendString(s"$remoteTopic/client/synack", "synack")
  }
}

object InboundConnectionActor {
  def props(localAddr: Address, remoteAddr: Address, eventListener: AssociationEventListener) = Props(classOf[InboundConnectionActor], localAddr, remoteAddr, eventListener)

  case class ListenAssociated(listener: HandleEventListener)

  case object DisconnectTimeout

}