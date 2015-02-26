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

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.remote.transport.AssociationHandle.HandleEventListener
import akka.remote.transport.Transport.AssociationEventListener
import akka.remote.transport.{AssociationHandle, Transport}
import akka.util.{ByteString, Timeout}
import com.typesafe.config.Config

import scala.concurrent.duration.Duration
import scala.concurrent.{Promise, _}
import scala.util.{Failure, Success}


class AmqpTransportSettings(config: Config) {

  import config._

  val Clientname: Option[String] = getString("client-name") match {
    case "" ⇒ None
    case value ⇒ Some(value)
  }

  val ConnectRetry = Duration(getDuration("connect-retry", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

  val ConnectTimeout = Duration(getDuration("connect-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

  val ShutdownTimeout = Duration(getDuration("shutdown-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

  val BrokerUrl = getString("broker.url")
  val BrokerUser = getString("broker.username")
  val BrokerPassword = getString("broker.password")
}

class AmqpTransport(val settings: AmqpTransportSettings, val system: ExtendedActorSystem) extends Transport {

  import org.leachbj.akka.mqlight.remote.AmqpTransport._
  import org.leachbj.akka.mqlight.remote.ListenServiceActor._

  def this(system: ExtendedActorSystem, conf: Config) = this(new AmqpTransportSettings(conf), system)

  implicit val executionContext = system.dispatcher

  @volatile private var localAddress: Address = _

  @volatile var listenService: ActorRef = _

  implicit val timeout = Timeout(settings.ConnectTimeout)

  override val schemeIdentifier: String = "amqp"
  override val maximumPayloadBytes: Int = 32000

  override def isResponsibleFor(address: Address): Boolean = true

  override def shutdown(): Future[Boolean] =
    (listenService ? ListenServiceActor.Shutdown).mapTo[Boolean]

  override def listen: Future[(Address, Promise[AssociationEventListener])] = {

    println(s"${settings.Clientname}: listen")

    val promise = Promise[Future[(Address, Promise[AssociationEventListener])]]()

    val listen: ActorRef = system.actorOf(ListenServiceActor.props(settings))
    listen ? Listen(settings.Clientname, settings.BrokerUrl, settings.BrokerUser, settings.BrokerPassword) onComplete {
      case Success(ListenStarted(address, listenFuture)) =>
        localAddress = address
        promise.success(listenFuture)
      case Success(_) => promise.failure(new RuntimeException("Unexpected response from mqlight client"))
      case Failure(e) => promise.failure(e)
    }

    Await.result(promise.future, settings.ConnectTimeout)
  }

  override def associate(remoteAddr: Address): Future[AssociationHandle] = {
    val promise = Promise[AssociationHandle]()

    system.systemActorOf(OutboundConnectionActor.props(localAddress, remoteAddr, promise), s"amqp-transport-client-${hostname(remoteAddr)}")

    promise.future
  }
}

object AmqpTransport {
  def hostname(address: Address) = address.host match {
    case Some(hostname) => hostname
    case None => ""
  }

  def addressName(address: Address) = s"${address.system}@${hostname(address)}"
}

case object AmqpConnectionDisassociate

class AmqpAssociationHandle(val localAddress: Address,
                            val remoteAddress: Address,
                            private val mqlightClient: ActorRef,
                            private val topic: String,
                            private val connection: ActorRef) extends AssociationHandle {

  import org.leachbj.akka.mqlight.client.MqLightClient._

  @volatile var available = true

  override val readHandlerPromise: Promise[HandleEventListener] = Promise()

  override def disassociate(): Unit = connection ! AmqpConnectionDisassociate

  override def write(payload: ByteString): Boolean = {
    if (available) {
      mqlightClient ! MqLightSendBytes(s"$topic/write", payload)
      true
    } else {
      println("not writable")
      false
    }
  }
}