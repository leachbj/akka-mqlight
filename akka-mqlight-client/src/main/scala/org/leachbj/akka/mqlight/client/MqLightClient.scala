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
package org.leachbj.akka.mqlight.client

import akka.actor._
import akka.util.ByteString
import com.ibm.mqlight.api
import com.ibm.mqlight.api._
import com.ibm.mqlight.api.endpoint.Endpoint
import com.ibm.mqlight.api.impl.callback.ThreadPoolCallbackService
import com.ibm.mqlight.api.impl.endpoint.SingleEndpointService
import com.ibm.mqlight.api.impl.timer.TimerServiceImpl
import com.ibm.mqlight.api.network.{NetworkChannel, NetworkListener, NetworkService}
import org.leachbj.akka.mqlight.client.ActorCompletionListener.{CompletionError, CompletionSuccess}
import org.leachbj.akka.mqlight.client.MqlightNetworkService.MqLightConnect

class MqLightClient(listener: Option[ActorRef]) extends Actor with ActorLogging with Stash {

  import org.leachbj.akka.mqlight.client.MqLightClient._

  val network = context.system.actorOf(Props[MqlightNetworkService])

  var client: NonBlockingClient = _

  override def postStop(): Unit = client.stop(None.orNull, None.orNull)

  override def receive: Receive = unconnected

  def unconnected: Receive = {
    case MqLightStart(clientId, url, username, password) =>
      log.debug("creating client to {}", url)
      client = createClient(clientId, url, username, password)
      context.become(waitingForStart(sender()))
  }

  def waitingForStart(notify: ActorRef): Receive = {
    case Started =>
      log.debug("connected to server")
      notify ! MqLightStarted(client.getId)
      context.become(started)
  }

  def started: Receive = {
    case MqLightSubscribe(topic) =>
      client.subscribe(topic, ActorDestinationListener, ActorCompletionListener, sender())
    case MqLightUnSubscribe(topic) =>
      client.unsubscribe(topic, ActorCompletionListener, sender())
    case MqLightSendString(topic, body) =>
      val writeReady = client.send(topic, body, None.orNull)
      if (!writeReady) context.become(waitingForDrain)
    case MqLightSendBytes(topic, body) =>
      val writeReady = client.send(topic, body.asByteBuffer, None.orNull, ActorCompletionListener, self)
      if (!writeReady) {
        //        log.warning("{} Full", this)
        //        writeable.drainPermits()
        listener.map(_ ! MqLightDraining)
        context.become(waitingForDrain)
      }
    case CompletionSuccess | _: CompletionError =>
    //      log.warning("{}", writeable.toString)
  }

  def waitingForDrain: Receive = {
    case Drain =>
      log.warning("{} Drained", this)
      //      writeable.set(2)
      unstashAll()
      listener.map(_ ! MqLightDrained)
      context.become(started)
    case CompletionSuccess | _: CompletionError =>
    //      log.warning("{}", writeable.toString)
    case any: Any =>
      log.warning("{} Client draining; stashing {}", this, any)
      stash()
  }

  private def createClient(clientId: Option[String], url: String, username: String, password: String) = {
    val clientOptions = {
      val opts = ClientOptions.builder().setCredentials(username, password)
      clientId.foreach(opts.setId)
      opts.build()
    }

    NonBlockingClient.create(new SingleEndpointService(url, username, password, None.orNull, false),
      new ThreadPoolCallbackService(5),
      new NetworkService() {
        override def connect(endpoint: Endpoint, listener: NetworkListener, promise: api.Promise[NetworkChannel]): Unit = {
          log.debug("connect: {}:{}", endpoint.getHost, endpoint.getPort)
          network ! MqLightConnect(endpoint, listener, promise)
        }
      },
      new TimerServiceImpl,
      None.orNull,
      clientOptions,
      new NonBlockingClientListener[ActorRef] {
        override def onStarted(nonBlockingClient: NonBlockingClient, t: ActorRef): Unit = t ! Started

        override def onRetrying(nonBlockingClient: NonBlockingClient, t: ActorRef, e: ClientException): Unit = t ! Retrying(e)

        override def onRestarted(nonBlockingClient: NonBlockingClient, t: ActorRef): Unit = t ! Restarted

        override def onStopped(nonBlockingClient: NonBlockingClient, t: ActorRef, e: ClientException): Unit = t ! Stopped

        override def onDrain(nonBlockingClient: NonBlockingClient, t: ActorRef): Unit = t ! Drain
      }, self)
  }
}

object MqLightClient {
  def props(listener: Option[ActorRef] = None) = Props(classOf[MqLightClient], listener)

  sealed trait MqLightEvent

  case object Started extends MqLightEvent

  case class Retrying(e: ClientException) extends MqLightEvent

  case object Restarted extends MqLightEvent

  case object Stopped extends MqLightEvent

  case object Drain extends MqLightEvent

  sealed trait MqLightCommand

  case class MqLightStart(clientId: Option[String], url: String, username: String, password: String) extends MqLightCommand

  case class MqLightStarted(clientId: String) extends MqLightCommand

  case class MqLightSubscribe(topic: String) extends MqLightCommand

  case class MqLightUnSubscribe(topic: String) extends MqLightCommand

  case class MqLightSendString(topic: String, body: String) extends MqLightCommand

  case class MqLightSendBytes(topic: String, body: ByteString) extends MqLightCommand

  sealed trait MqLightNotification

  case object MqLightDraining extends MqLightNotification
  case object MqLightDrained extends MqLightNotification

}

object ActorDestinationListener extends DestinationListener[ActorRef] {
  override def onMessage(client: NonBlockingClient, context: ActorRef, delivery: Delivery): Unit = context ! Message(delivery)

  override def onMalformed(client: NonBlockingClient, context: ActorRef, delivery: MalformedDelivery): Unit = Malformed(delivery)

  override def onUnsubscribed(client: NonBlockingClient, context: ActorRef, topicPattern: String, share: String, error: Exception): Unit = Unsubscribed(topicPattern, share, error)

  sealed trait DestinationEvent

  case class Message(delivery: Delivery) extends DestinationEvent

  case class Malformed(delivery: MalformedDelivery) extends DestinationEvent

  case class Unsubscribed(topicPattern: String, share: String, error: Exception) extends DestinationEvent

}

object ActorCompletionListener extends CompletionListener[ActorRef] {
  override def onSuccess(client: NonBlockingClient, context: ActorRef): Unit = context ! CompletionSuccess

  override def onError(client: NonBlockingClient, context: ActorRef, exception: Exception): Unit = context ! CompletionError(exception)

  sealed trait CompletionEvent

  case object CompletionSuccess extends CompletionEvent

  case class CompletionError(exception: Exception) extends CompletionEvent

}