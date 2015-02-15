package org.leachbj.akka.remote.amqp

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.{Address, ExtendedActorSystem}
import akka.remote.transport.AssociationHandle.{Disassociated, HandleEventListener, InboundPayload}
import akka.remote.transport.Transport.{AssociationEventListener, InboundAssociation}
import akka.remote.transport.{AssociationHandle, Transport}
import akka.util.ByteString
import com.ibm.mqlight.api._
import com.typesafe.config.Config

import scala.concurrent.{Promise, _}
import scala.util.Try


class AmqpTransportSettings(config: Config) {
  import config._

  val Clientname: Option[String] = getString("clientname") match {
    case ""    ⇒ None
    case value ⇒ Some(value)
  }
}

class AmqpAssociationHandle(val localAddress: Address,
                             val remoteAddress: Address,
                             private val nonBlockingClient: NonBlockingClient,
                             private val topic: String) extends AssociationHandle {
  override val readHandlerPromise: Promise[HandleEventListener] = Promise()

  override def disassociate(): Unit = {
    println(s"${localAddress.host} disassociate")
    nonBlockingClient.send(s"$topic/disassociate", "disassociate", None.orNull)
  }

  override def write(payload: ByteString): Boolean = {
    println(s"${localAddress.host} sending to ${topic}/write")
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
      val topic = delivery.getTopic.substring(0, delivery.getTopic.lastIndexOf('/'))    // strip of the /connect
      val remoteIndex = topic.lastIndexOf('/')  // after last / is the remote name
      val hn = """(.*)@(.*)""".r

      val remoteClient = topic.substring(remoteIndex + 1)

      println(s"""${settings.Clientname} connection request on topic ${delivery.getTopic} from client $remoteClient""")
      val hn(systemname, remote) = remoteClient

      println(s"${settings.Clientname} received connection request from system: $systemname remote: $remote")

      val remoteAddr = Address(schemeIdentifier, systemname, remote, 0)

      val remoteTopic = s"${addressName(remoteAddr)}/${addressName(localAddress)}"

      val handle = new AmqpAssociationHandle(localAddress, remoteAddr, nonBlockingClient, s"${remoteTopic}/client")
      handle.readHandlerPromise.future.onSuccess {
        case listener: HandleEventListener =>
          val localTopic = s"${addressName(localAddress)}/${addressName(remoteAddr)}"

          println(s"${settings.Clientname} inbound connection system: $systemname remote: $remote subscribing to ${localTopic}/server/+")
          try {
            nonBlockingClient.subscribe(s"${localTopic}/server/+", messageListener(listener), None.orNull, None.orNull)
          } catch {
            case e: StateException =>
              println(s"${settings.Clientname} already subscribed to ${topic}/+")
          }
      }
      association.notify(InboundAssociation(handle))
    }
  }

  def messageListener(listener: HandleEventListener) = new DestinationAdapter[Any] {
    override def onMessage(nonBlockingClient: NonBlockingClient, t: Any, delivery: Delivery): Unit = {
      delivery match {
        case bytes: BytesDelivery =>
          println(s"${settings.Clientname} data received on ${delivery.getTopic}")
          listener.notify(InboundPayload(ByteString.fromByteBuffer(bytes.getData)))
        case string: StringDelivery =>
          println(s"${settings.Clientname} string '${string.getData}' received on ${delivery.getTopic}")
          string.getData match {
            case "disassociate" =>
              listener.notify(Disassociated(AssociationHandle.Shutdown))
            case _ =>
              println(s"${settings.Clientname} unknown command topic ${delivery.getTopic}")
          }

        case msg: Any =>
          println(s"whats this $msg")
      }
    }
  }

  override def listen: Future[(Address, Promise[AssociationEventListener])] = {
    println(s"${settings.Clientname}: listen")
    val listenPromise = Promise[(Address, Promise[AssociationEventListener])]

    val clientOptions = ClientOptions.builder().setCredentials("admin", "password")
    settings.Clientname.foreach(clientOptions.setId(_))

    client = NonBlockingClient.create("amqp://localhost:5672", clientOptions.build(),
      new NonBlockingClientListener[AmqpTransport] {
        override def onStarted(nonBlockingClient: NonBlockingClient, t: AmqpTransport): Unit = {
          localAddress = Address(schemeIdentifier, system.name, settings.Clientname.getOrElse(client.getId), 0)

          associationListenerPromise.future.onSuccess {
            case eventListener =>
              val topic = addressName(localAddress)
              println(s"${settings.Clientname} listening on subscription ${topic}/+/connect")
              nonBlockingClient.subscribe(s"${topic}/+/connect", connectionListener(eventListener), None.orNull, None.orNull)
          }

          listenPromise.success((localAddress, associationListenerPromise))
        }

        override def onRetrying(nonBlockingClient: NonBlockingClient, t: AmqpTransport, e: ClientException): Unit = {
          println("retrying")
          e.printStackTrace()
        }

        override def onRestarted(nonBlockingClient: NonBlockingClient, t: AmqpTransport): Unit = println("restarted")

        override def onStopped(nonBlockingClient: NonBlockingClient, t: AmqpTransport, e: ClientException): Unit = {
          println("stopped")
          e.printStackTrace()
        }

        override def onDrain(nonBlockingClient: NonBlockingClient, t: AmqpTransport): Unit = println("draining")
      }, this)

    listenPromise.future
  }

  def addressName(address: Address) = s"${address.system}@${address.host.get}"

  override def shutdown(): Future[Boolean] = ???

  override def associate(remoteAddress: Address): Future[AssociationHandle] = {
    val remoteTopic = s"${addressName(remoteAddress)}/${addressName(localAddress)}"
    println(s"${settings.Clientname} associating with topic ${remoteTopic}")

    val handle = new AmqpAssociationHandle(localAddress, remoteAddress, client, s"${remoteTopic}/server")
    handle.readHandlerPromise.future.onSuccess {
      case listener: HandleEventListener =>

        val localTopic = s"${addressName(localAddress)}/${addressName(remoteAddress)}"
        println(s"${settings.Clientname} associate read handle ready on subscribing to $localTopic/client/+")

        client.subscribe(s"$localTopic/client/+", messageListener(listener), new CompletionListener[Any] {
          override def onSuccess(nonBlockingClient: NonBlockingClient, t: Any): Unit = {
          }

          override def onError(nonBlockingClient: NonBlockingClient, t: Any, e: Exception): Unit = {
            e.printStackTrace()
          }
        }, None.orNull)
    }

    val promise = Promise[AssociationHandle]

    client.send(s"$remoteTopic/connect", "", None.orNull, new CompletionListener[Any] {
      override def onSuccess(nonBlockingClient: NonBlockingClient, t: Any): Unit = {
        import scala.concurrent.duration._
        system.scheduler.scheduleOnce(1 second,
          new Runnable() {
            def run() = {
              promise.success(handle)
            }
          })
      }

      override def onError(nonBlockingClient: NonBlockingClient, t: Any, e: Exception): Unit = promise.failure(e)
    }, None.orNull)

    promise.future
  }

}