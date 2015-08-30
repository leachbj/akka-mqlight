package org.leachb.akka.mqlight.plug.points.network

import java.io.{File, FileInputStream}
import java.lang
import java.nio.ByteBuffer
import java.security.KeyStore
import javax.net.ssl.{SSLContext, SSLParameters, TrustManagerFactory}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.io._
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl._
import akka.util.ByteString
import com.ibm.mqlight.api.Promise
import com.ibm.mqlight.api.endpoint.Endpoint
import com.ibm.mqlight.api.network.{NetworkChannel, NetworkListener, NetworkService}
import io.netty.buffer.Unpooled

import scala.concurrent.Future
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
 * [[NetworkService]] implementation that uses akka-streams for the underlying
 * transport.
 *
 * @param system ActorSystem required for streams
 */
class StreamsNetworkService()(implicit system: ActorSystem) extends NetworkService {
  import org.leachb.akka.mqlight.plug.points.network.StreamsNetworkService._

  /**
   * Connect to the given [[Endpoint]] completing the provided [[Promise[NetworkChannel]] when
   * the connection is established and generate the required events to [[NetworkListener]].
   * @param endpoint the host port to connect to
   * @param listener receives network events once connected
   * @param promise completed when the connection is established
   */
  override def connect(endpoint: Endpoint, listener: NetworkListener, promise: Promise[NetworkChannel]): Unit = {
    system.log.debug("connecting to {}:{}", endpoint.getHost, endpoint.getPort)

    implicit val mat = ActorMaterializer()

    // a [[Flow[ByteString, ByteString, Future[OutgoingConnection]]]] where we will send outgoing data and receive incoming data,
    // will materialize a [[Future[OutgoingConnection]] that completes when connected
    val outgoingTry: Try[Flow[ByteString, ByteString, Future[OutgoingConnection]]] = networkFlowForEndpoint(endpoint)

    outgoingTry.recover {
      case e: Exception =>
        promise.setFailure(e)
      case t =>
        promise.setFailure(new Exception(t))
    }

    outgoingTry.foreach { outgoing =>
      // a [[NetworkChannelActorPublisher]] Source that materializes a [[NetworkChannel]]
      val source: Source[ByteString, NetworkChannel] =
        Source.actorPublisher[ByteString](Props[NetworkChannelActorPublisher]).mapMaterializedValue(StreamsNetworkChannel)

      // Join up the [[Publisher]] and [[Flow]] keeping both materialized values
      val sourceAndNetwork: Source[ByteString, (NetworkChannel, Future[OutgoingConnection])] = source.viaMat(outgoing)(Keep.both)

      // complete the graph by attaching a [[Sink]] to consume incoming [[ByteString]] events.  The [[SinkActor]] also
      // receives the [[NetworkChannel]] since it needs that to process the [[ByteString]] events so we use a [[Merge]]
      // to combine the materializedValue and events coming from the network into a single stream.
      val graph: RunnableGraph[(NetworkChannel, Future[OutgoingConnection])] = FlowGraph.closed(sourceAndNetwork) { implicit builder => src =>
        import FlowGraph.Implicits._

        // [[Any]] is really [[NetworkChannel Or ByteString]]
        val merge = builder.add(Merge[Any](2))

        // output all the network data to the merge
        src ~> merge

        // output the channel to to the merge
        builder.materializedValue.map {
          case (c, connection) => c
        } ~> merge

        // complete the graph with the SinkActor who consumers a NetworkChannel followed by ByteStrings
        val sink = Sink.actorRef[Any](system.actorOf(SinkActor.props(listener)), SinkActor.Done)
        merge ~> sink
      }

      // run the graph to obtain the [[NetworkChannel]] and [[Future[OutgoingConnection]]
      val (channel, future) = graph.run()

      // when the [[Future[OutgoingConnection]] completes complete the provided [[Promise]]
      implicit val exec = system.dispatcher
      future.onComplete {
        case Success(connection) =>
          system.log.debug("Connected to {}", connection)
          promise.setSuccess(channel)
        case Failure(e: Exception) => promise.setFailure(e)
        case Failure(t) => promise.setFailure(new Exception(t))
      }
    }
  }

  private[this] def networkFlowForEndpoint(endpoint: Endpoint): Try[Flow[ByteString, ByteString, Future[OutgoingConnection]]] = {
    val tcp: Flow[ByteString, ByteString, Future[OutgoingConnection]] = Tcp().outgoingConnection(endpoint.getHost, endpoint.getPort)

    if (endpoint.useSsl()) {
      val tlsWrap = Flow[ByteString].map(SendBytes)
      val tlsUnwrap = Flow[SslTlsInbound].collect { case SessionBytes(_, bytes) => bytes }
      val tlsBridge = BidiFlow.wrap(tlsWrap, tlsUnwrap)(Keep.none)

      for {
        trustManager <- getTrustManagerFactoryForCertChainFile(Option(endpoint.getCertChainFile))
        sslCtx <- getSslContext(trustManager)
        firstSession <- firstSession(sslCtx, endpoint.getVerifyName)
      } yield tlsBridge.atop(SslTls(sslCtx, firstSession, Role.client)).joinMat(tcp)(Keep.right)
    } else Success(tcp)
  }

  private[this] def getTrustManagerFactoryForCertChainFile(certChainFile: Option[File]): Try[TrustManagerFactory] = {
    def loadKeyStore(in: FileInputStream) = Try {
      val jks = try {
        val store = KeyStore.getInstance("JKS")
        store.load(in, null)
        store
      } finally {
        in.close()
      }

      jks
    }

    def trustManagerFactoryFromKeyStore(keyStore: KeyStore) = Try {
      val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      trustManagerFactory.init(keyStore)
      trustManagerFactory
    }

    // load the JKS if one was provided or provide a default TrustManagerFactory
    val loadedTrustManager = certChainFile.map(file => Try(new FileInputStream(file))).map(_.flatMap(loadKeyStore).flatMap(trustManagerFactoryFromKeyStore))
    loadedTrustManager.getOrElse(trustManagerFactoryFromKeyStore(None.orNull))
  }

  private[this] def getSslContext(trustManagerFactory: TrustManagerFactory): Try[SSLContext] = {
    for {
      sslContext <- Try(SSLContext.getInstance("TLS"))
      init <- Try {
        sslContext.init(null, trustManagerFactory.getTrustManagers, null)
        sslContext
      }
    } yield init
  }

  private[this] def firstSession(sslContext: SSLContext, verifyName: Boolean): Try[NegotiateNewSession] = {
    def filter(list: Array[String], pattern: Regex) =
      list.filter(pattern.findFirstIn(_).isEmpty).toList.toSeq

    val sslParams = if (verifyName) {
      val sslParams = new SSLParameters()
      sslParams.setEndpointIdentificationAlgorithm("HTTPS")
      Some(sslParams)
    } else None

    for {
      engine <- Try(sslContext.createSSLEngine())
    } yield NegotiateNewSession(
      Option(filter(engine.getEnabledCipherSuites, DisabledCipherPattern)),
      Option(filter(engine.getEnabledProtocols, DisabledProtocolPattern)),
      None,
      sslParams)
  }
}

object StreamsNetworkService {
  import org.leachb.akka.mqlight.plug.points.network.NetworkChannelActorPublisher.{MqlightClose, MqlightWrite}

  private case class StreamsNetworkChannel(actorRef: ActorRef) extends NetworkChannel() {
    var context: AnyRef = _

    override def write(buffer: ByteBuffer, promise: Promise[lang.Boolean]): Unit = actorRef ! MqlightWrite(ByteString.fromByteBuffer(buffer), promise)

    override def close(promise: Promise[Void]): Unit = actorRef ! MqlightClose(promise)

    override def getContext: AnyRef = context

    override def setContext(c: AnyRef): Unit = context = c
  }

  private class SinkActor(listener: NetworkListener) extends Actor {
    def receive: Receive = {
      case channel: NetworkChannel =>
        context.become(initialised(channel))
    }
    def initialised(channel: NetworkChannel): Receive = {
      case b: ByteString =>
        listener.onRead(channel, Unpooled.wrappedBuffer(b.asByteBuffer))
      case SinkActor.Done =>
        listener.onClose(channel)
      case Failure(e: Exception) =>
        listener.onError(channel, e)
      case Failure(t) =>
        listener.onError(channel, new Exception(t))
    }
  }

  private object SinkActor {
    def props(listener: NetworkListener) = Props(new SinkActor(listener))

    case object Done
  }

  /* Pattern of protocols to disable */
  private val DisabledProtocolPattern = "(SSLv2|SSLv3).*".r
  /* Pattern of cipher suites to disable */
  private val DisabledCipherPattern = ".*_(NULL|EXPORT|DES|RC4|MD5|PSK|SRP|CAMELLIA)_.*".r
}
