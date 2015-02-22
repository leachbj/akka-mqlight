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

import java.io.IOException
import java.lang
import java.net.InetSocketAddress
import java.nio.ByteBuffer

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.io.Tcp.{CommandFailed, Connected}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.ibm.mqlight.api
import com.ibm.mqlight.api.endpoint.Endpoint
import com.ibm.mqlight.api.network.{NetworkChannel, NetworkListener}

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