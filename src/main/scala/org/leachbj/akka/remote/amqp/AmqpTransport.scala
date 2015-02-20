package org.leachbj.akka.remote.amqp

import java.io.IOException
import java.lang
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.{Semaphore, TimeUnit}

import akka.actor._
import akka.io.Tcp.{CommandFailed, Connected}
import akka.io.{IO, Tcp}
import akka.remote.transport.AssociationHandle.{Disassociated, HandleEventListener, InboundPayload}
import akka.remote.transport.Transport.{AssociationEventListener, InboundAssociation}
import akka.remote.transport.{AssociationHandle, Transport}
import akka.util.{ByteString, Timeout}
import com.ibm.mqlight.api
import com.ibm.mqlight.api._
import com.ibm.mqlight.api.endpoint.Endpoint
import com.ibm.mqlight.api.impl.callback.ThreadPoolCallbackService
import com.ibm.mqlight.api.impl.endpoint.SingleEndpointService
import com.ibm.mqlight.api.impl.timer.TimerServiceImpl
import com.ibm.mqlight.api.network.{NetworkChannel, NetworkListener, NetworkService}
import com.typesafe.config.Config
import org.leachbj.akka.remote.amqp.ActorCompletionListener.{CompletionError, CompletionSuccess}
import org.leachbj.akka.remote.amqp.MqlightNetworkService.MqLightConnect

import scala.concurrent.duration.Duration
import scala.concurrent.{Promise, _}
import scala.util.{Failure, Success}


class AmqpTransportSettings(config: Config) {
  import config._

  val Clientname: Option[String] = getString("client-name") match {
    case ""    ⇒ None
    case value ⇒ Some(value)
  }

  val ConnectRetry = Duration(getDuration("connect-retry", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

  val ConnectTimeout = Duration(getDuration("connect-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
}

class AmqpAssociationHandle(val localAddress: Address,
                             val remoteAddress: Address,
                             private val nonBlockingClient: ActorRef,
                             private val writeable: Semaphore,
                             private val topic: String,
                             private val actor: ActorRef) extends AssociationHandle {
  import org.leachbj.akka.remote.amqp.MqLightClient._

  override val readHandlerPromise: Promise[HandleEventListener] = Promise()

  override def disassociate(): Unit = {
    actor ! Disassociate
  }

  override def write(payload: ByteString): Boolean = {
//    println(s"$this $writeable")
    if (writeable.tryAcquire(1)) {
      nonBlockingClient ! MqLightSendBytes(s"$topic/write", payload)
      true
    } else {
      println(s"$this failed write")
      false
    }
  }
}

case object Disassociate

class AmqpTransport(val settings: AmqpTransportSettings, val system: ExtendedActorSystem) extends Transport {
  import org.leachbj.akka.remote.amqp.AmqpTransport._

  def this(system: ExtendedActorSystem, conf: Config) = this(new AmqpTransportSettings(conf), system)

  implicit val executionContext = system.dispatcher

  override val schemeIdentifier: String = "amqp"
  override val maximumPayloadBytes: Int = 32000

  override def isResponsibleFor(address: Address): Boolean = true

  @volatile private var client: ActorRef = _
  @volatile private var localAddress: Address = _
  @volatile private var writeable: Semaphore = _

  override def shutdown(): Future[Boolean] = ???

  override def listen: Future[(Address, Promise[AssociationEventListener])] = {
    import akka.pattern.ask
    import org.leachbj.akka.remote.amqp.MqLightClient._

    println(s"${settings.Clientname}: listen")
    val listenPromise = Promise[(Address, Promise[AssociationEventListener])]()

    implicit val timeout = Timeout(settings.ConnectTimeout)

    client = system.systemActorOf(MqLightClient.props(), "amqp-mqlight-client")
    client ? MqLightStart(settings.Clientname, "amqp://localhost:5672", "admin", "password") onComplete {
      case Success(MqLightStarted(clientId, writeLock)) =>
        localAddress = Address(schemeIdentifier, system.name, clientId.replaceAll("_", "-"), 0)
        writeable = writeLock
        system.systemActorOf(ListenActor.props(client, writeable, localAddress, listenPromise), "amqp-transport-listen")
      case Success(_) => listenPromise.failure(new RuntimeException("Unexpected response from mqlight client"))
      case Failure(e) => listenPromise.failure(e)
    }

    listenPromise.future
  }

  override def associate(remoteAddr: Address): Future[AssociationHandle] = {
    val promise = Promise[AssociationHandle]()

    system.systemActorOf(ClientConnectionActor.props(client, writeable, localAddress, remoteAddr, promise), s"amqp-transport-client-${hostname(remoteAddr)}")

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

class MqLightClient extends Actor with ActorLogging with Stash {
  import org.leachbj.akka.remote.amqp.MqLightClient._

  val network = createSystemActor(Props[MqlightNetworkService], "amqp-transport-io")

  val writeable = new Semaphore(3)

  override def receive: Receive = unconnected

  def unconnected: Receive = {
    case MqLightStart(clientId, url, username, password) =>
      log.debug("creating client to {}", url)
      val client = createClient(clientId, url, username, password)
      context.become(waitingForStart(client, sender()))
  }

  def waitingForStart(client: NonBlockingClient, notify: ActorRef): Receive = {
    case Started =>
      log.debug("connected to server")
      notify ! MqLightStarted(client.getId, writeable)
      context.become(started(client))
  }

  def started(client: NonBlockingClient): Receive = {
    case MqLightSubscribe(topic) =>
      client.subscribe(topic, ActorDestinationListener, ActorCompletionListener, sender())
    case MqLightUnSubscribe(topic) =>
      client.unsubscribe(topic, ActorCompletionListener, sender())
    case MqLightSendString(topic, body) =>
      val writeReady = client.send(topic, body, None.orNull)
      if (!writeReady) context.become(waitingForDrain(client))
    case MqLightSendBytes(topic, body) =>
      val writeReady = client.send(topic, body.asByteBuffer, None.orNull, ActorCompletionListener, self)
      if (!writeReady) {
//        log.warning("{} Full", this)
//        writeable.drainPermits()
        context.become(waitingForDrain(client))
      }
    case CompletionSuccess | _: CompletionError =>
      writeable.release(1)
//      log.warning("{}", writeable.toString)
  }

  def waitingForDrain(client: NonBlockingClient): Receive = {
    case Drain =>
      log.warning("{} Drained", this)
//      writeable.set(2)
      unstashAll()
      context.become(started(client))
    case CompletionSuccess | _: CompletionError =>
      writeable.release(1)
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

  private def createSystemActor(props: Props, actorName: String) = {
    context.system match {
      case system: ExtendedActorSystem => system.systemActorOf(props, actorName)
      case _ => sys.error("ExtendedActorSystem required by MqLightClient Actor")
    }
  }
}

object MqLightClient {
  def props() = Props[MqLightClient]

  sealed trait MqLightEvent
  case object Started extends MqLightEvent
  case class Retrying(e: ClientException) extends MqLightEvent
  case object Restarted extends MqLightEvent
  case object Stopped extends MqLightEvent
  case object Drain extends MqLightEvent

  sealed trait MqLightCommand
  case class MqLightStart(clientId: Option[String], url: String, username: String, password: String) extends MqLightCommand
  case class MqLightStarted(clientId: String, writeable: Semaphore) extends MqLightCommand
  case class MqLightSubscribe(topic: String) extends MqLightCommand
  case class MqLightUnSubscribe(topic: String) extends MqLightCommand
  case class MqLightSendString(topic: String, body: String) extends MqLightCommand
  case class MqLightSendBytes(topic: String, body: ByteString) extends MqLightCommand
}

case class ActorNetworkChannel(actor: ActorRef) extends NetworkChannel {
  import org.leachbj.akka.remote.amqp.MqlightConnector._

  var context: AnyRef = _

  override def close(promise: api.Promise[Void]): Unit = {
    actor ! MqlightClose(promise)
  }
  override def write(buffer: ByteBuffer, promise: api.Promise[lang.Boolean]): Unit = {
    actor ! MqlightWrite(ByteString.fromByteBuffer(buffer), promise)
  }
  override def getContext: AnyRef = context
  override def setContext(c: AnyRef): Unit = context = c
}

class MqlightConnector(endpoint: Endpoint, listener: NetworkListener, connectPromise: api.Promise[NetworkChannel]) extends Actor with ActorLogging {
  import context.system
  import org.leachbj.akka.remote.amqp.MqlightConnector._

  override def preStart(): Unit = IO(Tcp) ! Tcp.Connect(new InetSocketAddress(endpoint.getHost, endpoint.getPort))

  override def receive = {
    case _: Connected =>
      log.debug("connected to {}:{}", endpoint.getHost, endpoint.getPort)
      val channel = ActorNetworkChannel(self)
      connectPromise.setSuccess(channel)
      sender() ! Tcp.Register(self)
      context.become(connected(channel, sender()))
    case failed: CommandFailed =>
      connectPromise.setFailure(new IOException(s"Can't connect to ${failed.cmd.failureMessage}"))
      context.stop(self)
  }

  def connected(channel: ActorNetworkChannel, connection: ActorRef): Receive = {
    case Tcp.Received(buffer) =>
      listener.onRead(channel, buffer.asByteBuffer)

    case Tcp.PeerClosed | Tcp.ErrorClosed =>
      listener.onClose(channel)

    case MqlightClose(promise) =>
      connection ! Tcp.Close
      context.become(waitingForClose(promise))

    case MqlightWrite(buffer, promise) =>
      connection ! Tcp.Write(buffer, MqlightWriteAck(promise))

    case MqlightWriteAck(promise) =>
      promise.setSuccess(true)

    case failed@Tcp.CommandFailed(Tcp.Write(buffer, MqlightWriteAck(promise))) =>
      log.debug("write failed")
      promise.setFailure(new RuntimeException(s"Can't write to ${failed.cmd.failureMessage}"))
  }

  def waitingForClose(promise: api.Promise[Void]): Receive = {
    case Tcp.Closed =>
      promise.setSuccess(None.orNull)
      context.stop(self)
  }
}

object MqlightConnector {
  case class MqlightClose(promise: api.Promise[Void])
  case class MqlightWrite(buffer: ByteString, promise: api.Promise[lang.Boolean])

  case class MqlightWriteAck(promise: api.Promise[lang.Boolean]) extends Tcp.Event
}

class MqlightNetworkService extends Actor with ActorLogging {
  import org.leachbj.akka.remote.amqp.MqlightNetworkService._

  override def receive = {
    case MqLightConnect(endpoint, listener, promise) =>
      log.debug("connecting to {}:{}", endpoint.getHost, endpoint.getPort)
      context.actorOf(Props(classOf[MqlightConnector], endpoint, listener, promise))
  }
}

object MqlightNetworkService {
  case class MqLightConnect(endpoint: Endpoint, listener: NetworkListener, promise: api.Promise[NetworkChannel])
}

class ListenActor(client: ActorRef, writeable: Semaphore, localAddr: Address, promise: Promise[(Address, Promise[AssociationEventListener])]) extends Actor with Stash with ActorLogging {
  import context.dispatcher
  import org.leachbj.akka.remote.amqp.ActorCompletionListener._
  import org.leachbj.akka.remote.amqp.ActorDestinationListener._
  import org.leachbj.akka.remote.amqp.AmqpTransport._
  import org.leachbj.akka.remote.amqp.ListenActor._
  import org.leachbj.akka.remote.amqp.MqLightClient._

  val localTopic = addressName(localAddr)

  override def preStart(): Unit = {
    log.debug("{} subscribing to listen address", localAddr)
    client ! MqLightSubscribe(s"$localTopic/+/connect")
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

      context.system match {
        case extended: ExtendedActorSystem =>
          extended.systemActorOf(ServerConnectionActor.props(client, writeable, localAddr, remoteAddr, eventListener), s"amqp-transport-server-${hostname(remoteAddr)}")
        case _ =>
          require(requirement = false, "ExtendedActorSystem required")
      }


  }

  def parseTopic(deliveryTopic: String) = {
    // topic format is <local systemname/local client>@<remote systemname>/<remote client>/connect
    val hn(_, _, remotesystem, remoteclient) = deliveryTopic
    (remotesystem, remoteclient)
  }

  val hn = """(.*)@(.*)/(.*)@(.*)/connect""".r
}

object ListenActor {
  def props(client: ActorRef, writeable: Semaphore, localAddr: Address, promise: Promise[(Address, Promise[AssociationEventListener])]) = Props(classOf[ListenActor], client, writeable, localAddr, promise)

  case class ListenAssociated(eventListener: AssociationEventListener)
}

class ServerConnectionActor(client: ActorRef, writeable: Semaphore, localAddr: Address, remoteAddr: Address, eventListener: AssociationEventListener) extends Actor with Stash with ActorLogging {
  import context.dispatcher
  import org.leachbj.akka.remote.amqp.ActorCompletionListener._
  import org.leachbj.akka.remote.amqp.ActorDestinationListener._
  import org.leachbj.akka.remote.amqp.AmqpTransport._
  import org.leachbj.akka.remote.amqp.MqLightClient._
  import org.leachbj.akka.remote.amqp.ServerConnectionActor._

  import scala.concurrent.duration._

  val localTopic = s"${addressName(localAddr)}/${addressName(remoteAddr)}"
  val remoteTopic = s"${addressName(remoteAddr)}/${addressName(localAddr)}"

  override def preStart(): Unit = client ! MqLightSubscribe(s"$localTopic/server/+")

  override def receive: Receive = waitForSubscribed

  def waitForSubscribed: Receive = {
    case CompletionSuccess =>
      sendSynAck()

      val handle = new AmqpAssociationHandle(localAddr, remoteAddr, client, writeable, s"$remoteTopic/client", self)
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
      listener.notify(Disassociated(AssociationHandle.Shutdown))
      context.become(disconnecting)
    case Disassociate =>
      log.debug("{} disassociate", localAddr)
      client ! MqLightSendString(s"$remoteTopic/client/disassociate", "disassociate")
      val timeout = context.system.scheduler.scheduleOnce(500.milliseconds, self, DisconnectTimeout)
      context.become(startedDisconnecting(timeout))
  }

  def startedDisconnecting(timeout: Cancellable): Receive = {
    case CompletionSuccess =>
    case CompletionError(e) =>
    case Message(delivery: StringDelivery) if delivery.getData == "disassociate" =>
      timeout.cancel()
      context.become(disconnecting)
    case DisconnectTimeout =>
      log.debug("{} timedout out waiting for disconnect", localAddr)
      context.become(disconnecting)
  }

  def disconnecting: Receive = {
    client ! MqLightUnSubscribe(s"$localTopic/server/+")

    {
      case CompletionSuccess =>
        log.debug("{} unsubscribed from {}", localAddr, remoteAddr)
        context.stop(self)
      case CompletionError(e) =>
        log.error(e, "{} disconnect/unsubscribe from {} error", localAddr, remoteAddr)
        context.stop(self)
    }
  }

  def sendSynAck() = {
    log.debug("{} sending synack to {}", localAddr, remoteAddr)
    client ! MqLightSendString(s"$remoteTopic/client/synack", "synack")
  }
}

object ServerConnectionActor {
  def props(client: ActorRef, writeable: Semaphore, localAddr: Address, remoteAddr: Address, eventListener: AssociationEventListener) = Props(classOf[ServerConnectionActor], client, writeable, localAddr, remoteAddr, eventListener)

  case class ListenAssociated(listener: HandleEventListener)
  case object DisconnectTimeout

}

class ClientConnectionActor(client: ActorRef, writeable: Semaphore, localAddr: Address, remoteAddr: Address, promise: Promise[AssociationHandle]) extends Actor with ActorLogging {
  import context.dispatcher
  import org.leachbj.akka.remote.amqp.ActorCompletionListener._
  import org.leachbj.akka.remote.amqp.ActorDestinationListener._
  import org.leachbj.akka.remote.amqp.AmqpTransport._
  import org.leachbj.akka.remote.amqp.ClientConnectionActor._
  import org.leachbj.akka.remote.amqp.MqLightClient._

  import scala.concurrent.duration._

  val localTopic = s"${addressName(localAddr)}/${addressName(remoteAddr)}"
  val remoteTopic = s"${addressName(remoteAddr)}/${addressName(localAddr)}"

  val settings = new AmqpTransportSettings(context.system.settings.config.getConfig("akka.remote.amqp"))

  override def preStart(): Unit = client ! MqLightSubscribe(s"$localTopic/client/+")

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

      val handle = new AmqpAssociationHandle(localAddr, remoteAddr, client, writeable, s"$remoteTopic/server", self)
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
      listener.notify(Disassociated(AssociationHandle.Shutdown))
      context.become(disconnecting)
    case Disassociate =>
      log.debug("{} disassociate", localAddr)
      client ! MqLightSendString(s"$remoteTopic/client/disassociate", "disassociate")
      val timeout = context.system.scheduler.scheduleOnce(500.milliseconds, self, DisconnectTimeout)
      context.become(startedDisconnecting(timeout))
  }

  def startedDisconnecting(timeout: Cancellable): Receive = {
    case CompletionSuccess =>
    case CompletionError(e) =>
    case Message(delivery: StringDelivery) if delivery.getData == "disassociate" =>
      timeout.cancel()
      context.become(disconnecting)
    case DisconnectTimeout =>
      log.debug("{} timed out out waiting for disconnect", localAddr)
      context.become(disconnecting)
  }

  def disconnecting: Receive = {
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

  private[this] def sendConnect() = {
    log.debug("{} sending connect request to {}", localAddr, remoteAddr)
    client ! MqLightSendString(s"$remoteTopic/connect", "connect")
  }
}

object ClientConnectionActor {
  def props(client: ActorRef, writeable: Semaphore, localAddr: Address, remoteAddr: Address, promise: Promise[AssociationHandle]) =
    Props(classOf[ClientConnectionActor], client, writeable, localAddr, remoteAddr, promise)

  case object RetryConnect
  case class ReadHandleSuccess(listener: HandleEventListener)
  case object DisconnectTimeout
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