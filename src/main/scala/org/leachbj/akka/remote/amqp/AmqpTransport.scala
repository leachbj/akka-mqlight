package org.leachbj.akka.remote.amqp

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.{Address, ExtendedActorSystem}
import akka.remote.transport.AssociationHandle.{HandleEventListener, InboundPayload}
import akka.remote.transport.Transport.{AssociationEventListener, InboundAssociation}
import akka.remote.transport.{AssociationHandle, Transport}
import akka.util.ByteString
import com.ibm.mqlight.api._
import com.typesafe.config.Config

import scala.concurrent.{Promise, _}


class AmqpTransportSettings(config: Config) {
  import config._

  val Hostname: String = getString("hostname") match {
    case ""    ⇒ "127.0.0.1"
    case value ⇒ value
  }

  val PortSelector: Int = getInt("port")
}

class AmqpAssociationHandle(val localAddress: Address,
                             val remoteAddress: Address,
                             private val nonBlockingClient: NonBlockingClient,
                             private val topic: String) extends AssociationHandle {
  override val readHandlerPromise: Promise[HandleEventListener] = Promise()

  override def disassociate(): Unit = {
    nonBlockingClient.send(s"$topic/disassociate", "", None.orNull)
  }

  override def write(payload: ByteString): Boolean = {
    nonBlockingClient.send(s"$topic/write", payload.asByteBuffer, None.orNull)
    true
  }
}

class AmqpTransport(val settings: AmqpTransportSettings, val system: ExtendedActorSystem) extends Transport {
  def this(system: ExtendedActorSystem, conf: Config) = this(new AmqpTransportSettings(conf), system)

  implicit val executionContext = system.dispatcher

  override val schemeIdentifier: String = "amqp"
  override val maximumPayloadBytes: Int = 32000

  override def isResponsibleFor(address: Address): Boolean = true

  private val associationListenerPromise: Promise[AssociationEventListener] = Promise()

  @volatile private var client: NonBlockingClient = _
  @volatile private var localAddress: Address = _

  def connectionListener(association: AssociationEventListener) = new DestinationAdapter[Any] {
    override def onMessage(nonBlockingClient: NonBlockingClient, t: Any, delivery: Delivery): Unit = {
      val remoteIndex = delivery.getTopic.lastIndexOf('/')
      val hn = """amqp://(.*)@(.*):(\d+)""".r

      val hn(systemname, remote, port) = delivery.getTopic.substring(remoteIndex + 1).replaceAll("_", "/")

      println(s"received connection request from system: $systemname remote: $remote port $port")

      val remoteAddr = Address(schemeIdentifier, systemname, remote, port.toInt)

      val handle = new AmqpAssociationHandle(localAddress, remoteAddr, nonBlockingClient, s"${delivery.getTopic}")
      handle.readHandlerPromise.future.onSuccess {
        case listener: HandleEventListener =>
          println(s"inbound connection system: $systemname remote: $remote port $port subscribing to ${delivery.getTopic}/+")
          nonBlockingClient.subscribe(s"${delivery.getTopic}/+", messageListener(listener), None.orNull, None.orNull)
      }
      association.notify(InboundAssociation(handle))
    }
  }

  def messageListener(listener: HandleEventListener) = new DestinationAdapter[Any] {
    override def onMessage(nonBlockingClient: NonBlockingClient, t: Any, delivery: Delivery): Unit = {
      delivery match {
        case bytes: BytesDelivery =>
          listener.notify(InboundPayload(ByteString.fromByteBuffer(bytes.getData)))
      }
    }
  }

  override def listen: Future[(Address, Promise[AssociationEventListener])] = {
    val listenPromise = Promise[(Address, Promise[AssociationEventListener])]

    client = NonBlockingClient.create("amqp://localhost:5672", ClientOptions.builder().setCredentials("admin", "password").build(),
      new NonBlockingClientListener[AmqpTransport] {
        override def onStarted(nonBlockingClient: NonBlockingClient, t: AmqpTransport): Unit = {
          val address = Address(schemeIdentifier, system.name, settings.Hostname, settings.PortSelector)
          localAddress = address

          associationListenerPromise.future.onSuccess {
            case eventListener =>
              println(s"listening on subscription ${address.toString}/+")
              nonBlockingClient.subscribe(s"${address.toString}/+", connectionListener(eventListener), None.orNull, None.orNull)
          }

          listenPromise.success((address, associationListenerPromise))
        }

        override def onRetrying(nonBlockingClient: NonBlockingClient, t: AmqpTransport, e: ClientException): Unit = println("retrying")

        override def onRestarted(nonBlockingClient: NonBlockingClient, t: AmqpTransport): Unit = println("restarted")

        override def onStopped(nonBlockingClient: NonBlockingClient, t: AmqpTransport, e: ClientException): Unit = println("stopped")

        override def onDrain(nonBlockingClient: NonBlockingClient, t: AmqpTransport): Unit = println("draining")
      }, this)


//    client.start(None.orNull, None.orNull)

//    client.subscribe("topci")
    listenPromise.future
  }

  override def shutdown(): Future[Boolean] = ???

  override def associate(remoteAddress: Address): Future[AssociationHandle] = {
    val topic = s"""${remoteAddress.toString}/${localAddress.toString.replaceAll("/", "_")}"""

    val handle = new AmqpAssociationHandle(localAddress, remoteAddress, client, topic)
    handle.readHandlerPromise.future.onSuccess {
      case listener: HandleEventListener =>
        println("associate read handle ready")

        client.subscribe(s"""${localAddress.toString}/${remoteAddress.toString.replaceAll("/", "_")}/+""", messageListener(listener), new CompletionListener[Any] {
          override def onSuccess(nonBlockingClient: NonBlockingClient, t: Any): Unit = {
          }

          override def onError(nonBlockingClient: NonBlockingClient, t: Any, e: Exception): Unit = {
            e.printStackTrace()
          }
        }, None.orNull)
    }

    client.send(s"$topic", "", None.orNull)

    Future.successful(handle)
  }

  def addressToSocketAddress(addr: Address): Future[InetSocketAddress] = addr match {
    case Address(_, _, Some(host), Some(port)) ⇒ Future { blocking { new InetSocketAddress(InetAddress.getByName(host), port) } }
    case _                                     ⇒ Future.failed(new IllegalArgumentException(s"Address [$addr] does not contain host or port information."))
  }

}