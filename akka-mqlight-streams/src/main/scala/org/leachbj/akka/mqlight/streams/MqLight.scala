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
package org.leachbj.akka.mqlight.streams

import akka.actor._
import akka.stream.actor._
import akka.stream.scaladsl.{Sink, Source}
import com.ibm.mqlight.api._
import org.leachbj.akka.mqlight.streams.MqLightSink._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

case class MqLightMessage(body: String, topic: String, properties: Map[String, AnyRef])

object MqLight extends ExtensionId[MqLight] with ExtensionIdProvider {
  def apply()(implicit system: ActorSystem): MqLight = super.apply(system)

  override def get(system: ActorSystem): MqLight = super.get(system)

  def lookup() = MqLight

  def createExtension(system: ExtendedActorSystem): MqLight = new MqLight(system)
}

class MqLight(system: ExtendedActorSystem) extends Extension {
  def publisher(): Sink[MqLightMessage, ActorRef] = Sink.actorSubscriber[MqLightMessage](MqLightSink.props())

  def consumer(topic: String): Source[MqLightMessage, ActorRef] = Source.actorPublisher[MqLightMessage](MqLightSource.props(topic))
}

private [streams] class MqLightSource(topic: String) extends ActorPublisher[MqLightMessage] with ActorLogging {
  val client: NonBlockingClient = NonBlockingClient.create("amqp://admin:password@localhost:5672", new NonBlockingClientListener[ActorRef]() {
    override def onRetrying(client: NonBlockingClient, c: ActorRef, ex: ClientException): Unit = c ! Retrying(ex)
    override def onRestarted(client: NonBlockingClient, c: ActorRef): Unit = c ! Restarted
    override def onStopped(client: NonBlockingClient, c: ActorRef, ex: ClientException): Unit = c ! Stopped(ex)
    override def onStarted(client: NonBlockingClient, c: ActorRef): Unit = c ! Started
    override def onDrain(client: NonBlockingClient, c: ActorRef): Unit = c ! Drain
  }, self)

  val messages: mutable.Queue[StringDelivery] = mutable.Queue.empty

  override def postStop(): Unit = client.stop(None.orNull, None.orNull)

  override def receive = waitForStarted orElse clientErrors

  def waitForStarted: Receive = {
    case Started =>
      client.subscribe(topic, SubscribeOptions.builder().setCredit(5).setAutoConfirm(false).setQos(QOS.AT_LEAST_ONCE).build(), new DestinationListener[ActorRef] {
        override def onMessage(client: NonBlockingClient, context: ActorRef, delivery: Delivery): Unit = delivery match {
          case s: StringDelivery =>
            context ! s
        }
        override def onMalformed(client: NonBlockingClient, context: ActorRef, delivery: MalformedDelivery): Unit = ()
        override def onUnsubscribed(client: NonBlockingClient, context: ActorRef, topicPattern: String, share: String, error: Exception): Unit = ()
      }, new CompletionListener[ActorRef] {
        override def onError(client: NonBlockingClient, context: ActorRef, exception: Exception): Unit = context ! Error(exception)

        override def onSuccess(client: NonBlockingClient, context: ActorRef): Unit = context ! Success
      }, self)

      context.become(waitForSubscribed orElse clientErrors)

    case ActorPublisherMessage.Request(elements) =>
  }

  def waitForSubscribed: Receive = {
    case Error(ex) =>
    case Success =>
      context.become(processDeliveries orElse clientErrors)
    case ActorPublisherMessage.Request(elements) =>
  }

  def processDeliveries: Receive = {
    case s: StringDelivery =>
      messages.enqueue(s)
      signalOnNexts()

    case ActorPublisherMessage.Request(elements) =>
      signalOnNexts()

    case ActorPublisherMessage.Cancel =>
      context.stop(self)
  }

  @tailrec private def signalOnNexts(): Unit =
    if (messages.nonEmpty && totalDemand > 0) {
      val ready = messages.dequeue()

      onNext(MqLightMessage(ready.getData, ready.getTopic, ready.getProperties.asScala.toMap))

      try {
        ready.confirm()
      } catch {
        case ex: StateException =>
          log.error(ex, "Failed to confirm delivery")
          onErrorThenStop(ex)
      }

      if (totalDemand > 0) signalOnNexts()
    }

  def clientErrors: Receive = {
    case Retrying(ex) =>
      log.error(ex, "Unable to connect to broker, retrying")
    case Stopped(ex) =>
      log.error(ex, "Client stopped unexpectedly")
      onErrorThenStop(ex)
    case ActorPublisherMessage.Cancel =>
      context.stop(self)
  }
}

private [streams] object MqLightSource {
  def props(topic: String) = Props[MqLightSource](new MqLightSource(topic))
}

private [streams] class MqLightSink extends ActorSubscriber with ActorLogging {
  var inFlight: Int = 1

  val client: NonBlockingClient = NonBlockingClient.create("amqp://admin:password@localhost:5672", ClientOptions.builder.build, new NonBlockingClientListener[ActorRef]() {
    override def onRetrying(client: NonBlockingClient, c: ActorRef, ex: ClientException): Unit = c ! Retrying(ex)
    override def onRestarted(client: NonBlockingClient, c: ActorRef): Unit = c ! Restarted
    override def onStopped(client: NonBlockingClient, c: ActorRef, ex: ClientException): Unit = c ! Stopped(ex)
    override def onStarted(client: NonBlockingClient, c: ActorRef): Unit = c ! Started
    override def onDrain(client: NonBlockingClient, c: ActorRef): Unit = c ! Drain
  }, self)

  // we can only write out messages when we're ready
  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(1) {
    override val batchSize = 1
    override def inFlightInternally = inFlight
  }

  override def postStop(): Unit = client.stop(None.orNull, None.orNull)

  override def receive = waitForStarted orElse clientErrors

  def waitForStarted: Receive = {
    case Started =>
      inFlight -= 1
      context.become(processSubscription)
  }

  def clientErrors: Receive = {
    case Retrying(ex) =>
      log.error(ex, "Unable to connect to broker, retrying")
    case Stopped(ex) =>
      log.error(ex, "Client stopped unexpectedly")
      cancel()
  }

  def processSubscription: Receive = {
    case ActorSubscriberMessage.OnNext(message: MqLightMessage) =>
      try {
        inFlight += 1
        val sent = client.send(message.topic, message.body, message.properties.asJava, SendOptions.builder.build, new CompletionListener[ActorRef]() {
          def onSuccess(client: NonBlockingClient, context: ActorRef): Unit = context ! Success
          def onError(client: NonBlockingClient, context: ActorRef, exception: Exception): Unit = context ! Error(exception)
        }, self)
        if (sent) context.become(waitForCompletion orElse clientErrors)
        else context.become(waitForDrain orElse clientErrors)
      } catch {
        case ex: StoppedException =>
          log.error(ex, "Client stopped unexpectedly")
          cancel()
      }

    case ActorSubscriberMessage.OnError(cause) =>
      log.error(cause, "Tearing down MqLightSink due to upstream error")
      context.stop(self)

    case ActorSubscriberMessage.OnComplete =>
      context.stop(self)
  }

  def waitForCompletion: Receive = {
    case Success =>
      inFlight -= 1
      context.become(processSubscription orElse clientErrors)

    case Error(ex) =>
      log.error(ex, "Client stopped unexpectedly")
      cancel()

  }

  def waitForDrain: Receive = {
    case Drain =>
      context.become(waitForCompletion orElse clientErrors)
  }
}

private [streams] object MqLightSink {
  def props() = Props[MqLightSink]

  case object Success
  case class Error(error: Exception)

  case object Started
  case object Drain
  case object Restarted
  case class Retrying(error: Exception)
  case class Stopped(error: Exception)
}