[akka-mqlight-streams](http://github.com/leachbj/akka-mqlight-streams) is a
[Reactive Streams](http://www.reactive-streams.org/) driver for AMQP using
the [java-mqlight](https://github.com/mqlight/java-mqlight) API.

## Example

The `MqLight` extension provides a publisher and a consumer function
to obtain a streams `Sink` and `Source` respectively.

The following example will generate 5 messages to the test-topic
to the MqLight `Sink` and then consume 5 messages from the MqLight
`Source`.

*Note* the MqLight client is hard-coded to connect to a local broker
with fixed credentials.

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

## License

akka-mqlight-streams is Copyright 2015 Bernard Leach and [Licensed under the MIT license](http://opensource.org/licenses/MIT).

