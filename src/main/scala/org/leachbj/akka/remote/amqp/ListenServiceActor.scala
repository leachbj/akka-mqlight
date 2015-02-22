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
package org.leachbj.akka.remote.amqp

import akka.actor._
import akka.pattern.{ask, pipe}
import akka.remote.transport.Transport.AssociationEventListener
import akka.util.Timeout
import com.ibm.mqlight.api.{MalformedDelivery, StringDelivery}
import org.leachbj.akka.remote.amqp.ActorCompletionListener.{CompletionError, CompletionSuccess}
import org.leachbj.akka.remote.amqp.ActorDestinationListener._
import org.leachbj.akka.remote.amqp.MqLightClient._

import scala.concurrent.{Future, Promise}

class ListenServiceActor(settings: AmqpTransportSettings) extends Actor with ActorLogging with Stash {

  import context.dispatcher
  import org.leachbj.akka.remote.amqp.AmqpTransport._
  import org.leachbj.akka.remote.amqp.ListenServiceActor._

  implicit val timeout = Timeout(settings.ConnectTimeout)

  def receive: Receive = {
    case Listen(clientId, url, username, password) =>
      log.debug("{} connecting to {}", settings.Clientname, url)
      val client = context.system.actorOf(MqLightClient.props())
      client ? MqLightStart(clientId, url, username, password) pipeTo self
      context.become(waitForStart(client, sender()))
  }

  def waitForStart(client: ActorRef, actor: ActorRef): Receive = {
    case MqLightStarted(clientId) =>
      log.debug("{} client started {}", settings.Clientname, clientId)
      val listenPromise = Promise[(Address, Promise[AssociationEventListener])]()

      val localAddress = Address("amqp", context.system.name, clientId.replaceAll("_", "-"), 0)

      actor ! ListenStarted(localAddress, listenPromise.future)

      val localTopic = addressName(localAddress)
      log.debug("{} subscribing to listen address", localAddress)

      client ! MqLightSubscribe(s"$localTopic/+/connect")
      context.become(awaitSubscribe(client, listenPromise, localAddress))
//    case Failure(e) => actor ! Failure(e)
//    case any: AnyRef => log.error("{} wtf {}", settings.Clientname, any)
  }

  def awaitSubscribe(client: ActorRef, promise: Promise[(Address, Promise[AssociationEventListener])], localAddr: Address): Receive = {
    case CompletionSuccess =>
      log.debug("{} subscribed", localAddr)

      val associationListenerPromise: Promise[AssociationEventListener] = Promise()
      associationListenerPromise.future.onSuccess {
        case eventListener =>
          self ! ListenAssociated(eventListener)
      }

      promise.success((localAddr, associationListenerPromise))
      context.become(awaitAssociation(client, localAddr))
    case CompletionError(e) =>
      log.error(e, "{} subscription failed", localAddr)
      throw e
  }

  def awaitAssociation(client: ActorRef, localAddr: Address): Receive = {
    case ListenAssociated(listener) =>
      log.debug("{} listen associated", localAddr)
      unstashAll()
      context.become(associated(client, localAddr, listener))
    case _: Message =>
      log.debug("{} message received before associated", localAddr)
      stash()
    case Malformed(delivery: MalformedDelivery) =>
      log.debug("{} malformed message received before associated", localAddr)
    case Unsubscribed(topicPattern, share, error) =>
      log.error(error, "{} unsubscribed from {}", localAddr, topicPattern)
      throw error
  }

  def associated(client: ActorRef, localAddr: Address, eventListener: AssociationEventListener): Receive = {
    case Message(delivery: StringDelivery) if delivery.getData == "connect" =>
      log.debug("{} connect received on topic {}", localAddr, delivery.getTopic)
      val (remotesystem, remoteclient) = parseTopic(delivery.getTopic)
      val remoteAddr = Address("amqp", remotesystem, remoteclient, 0)

      log.debug("{} connect request {}", localAddr, remoteAddr)

      context.system.actorOf(InboundConnectionActor.props(localAddr, remoteAddr, eventListener))

    case Shutdown =>
      log.info("{} shutting down", localAddr)
      sender ! true
  }

  def parseTopic(deliveryTopic: String) = {
    // topic format is <local systemname/local client>@<remote systemname>/<remote client>/connect
    val hn(_, _, remotesystem, remoteclient) = deliveryTopic
    (remotesystem, remoteclient)
  }

  val hn = """(.*)@(.*)/(.*)@(.*)/connect""".r
}

object ListenServiceActor {
  def props(settings: AmqpTransportSettings): Props = Props(classOf[ListenServiceActor], settings)

  case object Shutdown

  case class Listen(clientId: Option[String], url: String, username: String, password: String)

  // internal api
  case class ListenStarted(localAddress: Address, future: Future[(Address, Promise[AssociationEventListener])])

  case class ListenAssociated(eventListener: AssociationEventListener)

}
