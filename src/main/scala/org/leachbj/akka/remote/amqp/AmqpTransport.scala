package org.leachbj.akka.remote.amqp

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.remote.transport.AssociationHandle.{Disassociated, HandleEventListener, InboundPayload}
import akka.remote.transport.Transport.{AssociationEventListener, InboundAssociation}
import akka.remote.transport.{AssociationHandle, Transport}
import akka.util.ByteString
import com.ibm.mqlight.api._
import com.typesafe.config.Config

import scala.concurrent.duration.Duration
import scala.concurrent.{Promise, _}


class AmqpTransportSettings(config: Config) {
  import config._

  val Clientname: Option[String] = getString("client-name") match {
    case ""    ⇒ None
    case value ⇒ Some(value)
  }

  val ConnectRetry = Duration(getDuration("connect-retry", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
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
//    println(s"$topic/write ${payload.length}")
    nonBlockingClient.getState
    val writeReady = nonBlockingClient.send(s"$topic/write", payload.asByteBuffer, None.orNull)
    if (!writeReady) println(s"$topic/write is full")
    true
  }
}

class AmqpTransport(val settings: AmqpTransportSettings, val system: ExtendedActorSystem) extends Transport {
  def this(system: ExtendedActorSystem, conf: Config) = this(new AmqpTransportSettings(conf), system)

  implicit val executionContext = system.dispatcher

  override val schemeIdentifier: String = "amqp"
  override val maximumPayloadBytes: Int = 32000

  override def isResponsibleFor(address: Address): Boolean = true

  @volatile private var client: NonBlockingClient = _
  @volatile private var localAddress: Address = _

  def addressName(address: Address) = s"${address.system}@${address.host.get}"

  override def shutdown(): Future[Boolean] = ???

  override def listen: Future[(Address, Promise[AssociationEventListener])] = {
    println(s"${settings.Clientname}: listen")
    val listenPromise = Promise[(Address, Promise[AssociationEventListener])]

    val clientOptions = ClientOptions.builder().setCredentials("admin", "password")
    settings.Clientname.foreach(clientOptions.setId(_))

    client = NonBlockingClient.create("amqp://localhost:5672", clientOptions.build(),
      new NonBlockingClientListener[AmqpTransport] {
        override def onStarted(nonBlockingClient: NonBlockingClient, t: AmqpTransport): Unit = {
          localAddress = Address(schemeIdentifier, system.name, settings.Clientname.getOrElse(client.getId), 0)

          system.systemActorOf(ListenActor.props(client, localAddress, listenPromise), s"amqp-transport-listen")
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

        override def onDrain(nonBlockingClient: NonBlockingClient, t: AmqpTransport): Unit = {
          println(s"${settings.Clientname}: draining")
          try {
            throw new RuntimeException()
          } catch {
            case e: RuntimeException => e.printStackTrace()
          }
        }
      }, this)

    listenPromise.future
  }

  override def associate(remoteAddr: Address): Future[AssociationHandle] = {
    val promise = Promise[AssociationHandle]

    system.systemActorOf(ClientConnectionActor.props(client, localAddress, remoteAddr, promise), s"amqp-transport-client-${remoteAddr.host.get}")

    promise.future
  }
}

class ListenActor(client: NonBlockingClient, localAddr: Address, promise: Promise[(Address, Promise[AssociationEventListener])]) extends Actor with Stash with ActorLogging {
  import context.dispatcher
  import org.leachbj.akka.remote.amqp.ActorCompletionListener._
  import org.leachbj.akka.remote.amqp.ActorDestinationListener._
  import org.leachbj.akka.remote.amqp.ListenActor._

  val localTopic = addressName(localAddr)

  override def preStart(): Unit = {
    log.debug("{} subscribing to listen address", localAddr)
    client.subscribe(s"${localTopic}/+/connect", ActorDestinationListener, ActorCompletionListener, self)
  }

  override def receive: Receive = awaitSubscribe

  def awaitSubscribe: Receive = {
    case CompletionSuccess =>
      log.debug("{} subscribed", localAddr)
      val associationListenerPromise: Promise[AssociationEventListener] = Promise()
      associationListenerPromise.future.onSuccess {
        case eventListener =>
          self ! ListenAssociated(eventListener)
      }

      promise.success((localAddr, associationListenerPromise))
      context.become(awaitAssociation)
    case CompletionError(e) =>
      log.error(e, "{} subscription failed", localAddr)
      throw e
  }

  def awaitAssociation: Receive = {
    case ListenAssociated(listener) =>
      log.debug("{} listen associated", localAddr)
      unstashAll()
      context.become(associated(listener))
    case _: Message =>
      log.debug("{} message received before associated", localAddr)
      stash()
    case Malformed(delivery: MalformedDelivery) =>
      log.debug("{} malformed message received before associated", localAddr)
    case Unsubscribed(topicPattern, share, error) =>
      log.error(error, "{} unsubscribed from {}", localAddr, topicPattern)
      throw error
  }

  def associated(eventListener: AssociationEventListener): Receive = {
    case Message(delivery: StringDelivery) if delivery.getData == "connect" =>
      log.debug("{} connect received on topic {}", localAddr, delivery.getTopic)
      val (remotesystem, remoteclient) = parseTopic(delivery.getTopic)
      val remoteAddr = Address("amqp", remotesystem, remoteclient, 0)

      log.debug("{} connect request {}", localAddr, remoteAddr)

      context.system.asInstanceOf[ExtendedActorSystem].systemActorOf(ServerConnectionActor.props(client, localAddr, remoteAddr, eventListener), s"amqp-transport-server-${remoteAddr.host.get}")

  }

  def parseTopic(deliveryTopic: String) = {
//    val topic = deliveryTopic.substring(0, deliveryTopic.lastIndexOf('/'))    // strip of the /connect
//    val remoteIndex = topic.lastIndexOf('/')                                  // after last / is the remote name
//    val remoteClient = topic.substring(remoteIndex + 1)
//    val hn(remotesystem, remoteclient) = remoteClient

    // topic format is <local systemname/local client>@<remote systemname>/<remote client>/connect
    val hn(localsystem, localclient, remotesystem, remoteclient) = deliveryTopic
    (remotesystem, remoteclient)
  }

  val hn = """(.*)@(.*)/(.*)@(.*)/connect""".r
}

object ListenActor {
  def props(client: NonBlockingClient, localAddr: Address, promise: Promise[(Address, Promise[AssociationEventListener])]) = Props(classOf[ListenActor], client, localAddr, promise)

  def addressName(address: Address) = s"${address.system}@${address.host.get}"

  case class ListenAssociated(eventListener: AssociationEventListener)
}

class ServerConnectionActor(client: NonBlockingClient, localAddr: Address, remoteAddr: Address, eventListener: AssociationEventListener) extends Actor with Stash with ActorLogging {
  import context.dispatcher
  import org.leachbj.akka.remote.amqp.ActorCompletionListener._
  import org.leachbj.akka.remote.amqp.ActorDestinationListener._
  import org.leachbj.akka.remote.amqp.ServerConnectionActor._



  val localTopic = s"${addressName(localAddr)}/${addressName(remoteAddr)}"
  val remoteTopic = s"${addressName(remoteAddr)}/${addressName(localAddr)}"

  override def preStart(): Unit = {
    client.subscribe(s"${localTopic}/server/+", ActorDestinationListener, ActorCompletionListener, self)
  }

  override def receive: Receive = waitForSubscribed

  def waitForSubscribed: Receive = {
    case CompletionSuccess =>
      sendSynAck()

      val handle = new AmqpAssociationHandle(localAddr, remoteAddr, client, s"${remoteTopic}/client")
      eventListener.notify(InboundAssociation(handle))

      handle.readHandlerPromise.future.onSuccess {
        case listener: HandleEventListener =>
          self ! ListenAssociated(listener)
      }

      context.become(awaitAssociation)
    case CompletionError(e) =>
      throw e
  }

  def awaitAssociation: Receive = {
    case ListenAssociated(listener) =>
      log.debug("{} listen associated", localAddr)
      unstashAll()
      context.become(associated(listener))
    case _: Message =>
      log.debug("{} message received before associated", localAddr)
      stash()
    case Malformed(delivery: MalformedDelivery) =>
      log.debug("{} malformed message received before associated", localAddr)
    case Unsubscribed(topicPattern, share, error) =>
      log.error(error, "{} unsubscribed from {}", localAddr, topicPattern)
      throw error
  }


  def associated(listener: HandleEventListener): Receive = {
    case Message(bytes: BytesDelivery) =>
      listener.notify(InboundPayload(ByteString.fromByteBuffer(bytes.getData)))
    case Message(delivery: StringDelivery) if delivery.getData == "disassociate" =>
      client.unsubscribe(s"$localTopic/client/+", ActorCompletionListener, self)
      listener.notify(Disassociated(AssociationHandle.Shutdown))
      context.become(disconnecting)
  }

  def disconnecting: Receive = {
    case CompletionSuccess =>
      log.debug("{} unsubscribed from {}", localAddr, remoteAddr)
      context.stop(self)
    case CompletionError(e) =>
      log.error(e, "{} disconnect/unsubscribe from {} error", localAddr, remoteAddr)
      context.stop(self)
  }

  def sendSynAck() = {
    log.debug("{} sending synack to {}", localAddr, remoteAddr)
    val writeReady = client.send(s"${remoteTopic}/client/synack", "synack", None.orNull)
    if (!writeReady) println(s"${remoteTopic}/client/synack is full")
  }
}

object ServerConnectionActor {
  def props(client: NonBlockingClient, localAddr: Address, remoteAddr: Address, eventListener: AssociationEventListener) = Props(classOf[ServerConnectionActor], client, localAddr, remoteAddr, eventListener)

  def addressName(address: Address) = s"${address.system}@${address.host.get}"

  case class ListenAssociated(listener: HandleEventListener)

}

class ClientConnectionActor(client: NonBlockingClient, localAddr: Address, remoteAddr: Address, promise: Promise[AssociationHandle]) extends Actor with ActorLogging {
  import context.dispatcher
  import org.leachbj.akka.remote.amqp.ActorCompletionListener._
  import org.leachbj.akka.remote.amqp.ActorDestinationListener._
  import org.leachbj.akka.remote.amqp.ClientConnectionActor._

  val localTopic = s"${addressName(localAddr)}/${addressName(remoteAddr)}"
  val remoteTopic = s"${addressName(remoteAddr)}/${addressName(localAddr)}"

  val settings = new AmqpTransportSettings(context.system.settings.config.getConfig("akka.remote.amqp"))

  override def preStart(): Unit =
    client.subscribe(s"$localTopic/client/+", ActorDestinationListener, ActorCompletionListener, self)

  override def receive: Receive = waitForSubscribed

  def waitForSubscribed: Receive = {
    case CompletionSuccess =>
      sendConnect()
      val retry = context.system.scheduler.schedule(settings.ConnectRetry, settings.ConnectRetry, self, RetryConnect)
      context.become(unconnected(retry))
    case CompletionError(e) =>
      throw e
  }

  def unconnected(retry: Cancellable): Receive = {
    case Message(delivery: StringDelivery) if delivery.getData == "synack" =>
      retry.cancel()
      log.debug("{} received synack from {}", localAddr, remoteAddr)

      val handle = new AmqpAssociationHandle(localAddr, remoteAddr, client, s"${remoteTopic}/server")
      handle.readHandlerPromise.future.onSuccess {
        case listener: HandleEventListener =>
          self ! ReadHandleSuccess(listener)
      }

      promise.success(handle)
      context.become(waitForReader)
    case RetryConnect =>
      log.debug("{} retry connect to {}", localAddr, remoteAddr)
      sendConnect()
  }

  def waitForReader: Receive = {
    case ReadHandleSuccess(listener) =>
      log.debug("{} read handle ready {}", localAddr, remoteAddr)
      context.become(connected(listener))
  }

  def connected(listener: HandleEventListener): Receive = {
    case Message(bytes: BytesDelivery) =>
      listener.notify(InboundPayload(ByteString.fromByteBuffer(bytes.getData)))
    case Message(delivery: StringDelivery) if delivery.getData == "disassociate" =>
      client.unsubscribe(s"$localTopic/client/+", ActorCompletionListener, self)
      listener.notify(Disassociated(AssociationHandle.Shutdown))
      context.become(disconnecting)
  }

  def disconnecting: Receive = {
    case CompletionSuccess =>
      log.debug("{} unsubscribed from {}", localAddr, remoteAddr)
      context.stop(self)
    case CompletionError(e) =>
      log.error(e, "{} disconnect/unsubscribe from {} error", localAddr, remoteAddr)
      context.stop(self)
  }

  private[this] def sendConnect() = {
    log.debug("{} sending connect request to {}", localAddr, remoteAddr)
    val writeReady = client.send(s"$remoteTopic/connect", "connect", None.orNull)
    if (!writeReady) println(s"${remoteTopic}/connect is full")
  }
}

object ClientConnectionActor {
  def props(client: NonBlockingClient, localAddr: Address, remoteAddr: Address, promise: Promise[AssociationHandle]) =
    Props(classOf[ClientConnectionActor], client, localAddr, remoteAddr, promise)

  def addressName(address: Address) = s"${address.system}@${address.host.get}"

  case object RetryConnect
  case class ReadHandleSuccess(listener: HandleEventListener)
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