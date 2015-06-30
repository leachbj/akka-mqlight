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
package samples

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.leachbj.akka.mqlight.streams.{MqLight, MqLightMessage}

import scala.concurrent.Future

object MqLightStreamsSample extends App {
  implicit val sys = ActorSystem("MqLighStreamsSample")
  implicit val mat = ActorMaterializer()
  implicit val dispatcher = sys.dispatcher

  val consumer: Source[MqLightMessage, ActorRef] = MqLight().consumer("test-topic")
  val producer: Sink[MqLightMessage, ActorRef] = MqLight().publisher()

  // consume 5 messages from test-topic and print them out
  val consumed: Future[Unit] = consumer.take(5).runForeach(println)

  // generate 5 messages and send them to the test-topic
  val produced: ActorRef = Source(0 to 5).map(x => MqLightMessage(s"message $x", "test-topic", Map.empty)).runWith(producer)

  consumed.onComplete {
    case _ =>
      sys.shutdown()
  }
}
