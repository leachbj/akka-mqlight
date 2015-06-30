[akka-mqlight](http://github.com/leachbj/akka-mqlight) provides the following APIs;

* akka-mqlight-client is a simple wrapper for the java
 [java-mqlight](https://github.com/mqlight/java-mqlight) library which
 is an AMQP1.0 client suitable for java clients accessing IBM's [MQ Light](https://developer.ibm.com/messaging/mq-light/).

* akka-mqlight-remote is an implementation of the Akka remote
  [Transport interface](http://doc.akka.io/api/akka/2.3.7/index.html#akka.remote.transport.Transport)
  interface.  This implementation is a drop-in replacement(*) for the existing TCP/UCP based Transport
  implementations and lets you use Akka remoting via an the MQ Light message broker.  This could be
  used in cases where direct network connections are not possible for instance in a PAAS type environment.

* akka-mqlight-streams is a [Reactive Streams](http://www.reactive-streams.org/) driver for AMQP using
  the [java-mqlight](https://github.com/mqlight/java-mqlight) API.

(*) This is very much a case of 'in theory', there's still the question of if this is even a good
idea in the first place!

## Getting Started

You should then be able to build the library with a simple `mvn install`.

Included in the `akka-mqlight-remote` test directory is a copy of the `TransformationApp` cluster
sample from Typesafe (see [transformation](https://github.com/akka/akka/tree/master/akka-samples/akka-sample-cluster-scala/src/main/scala/sample/cluster/transformation)).

After starting a local MQ Light instance you can start the `TransformationApp` by running
`mvn exec:exec` in the `akka-mqlight-remote` directory.

For a streams example take a look at `MqLighStreamsSample` in the test directory of the
`akka-mqlight-stream` module.

## Contributing

At this point I would consider this implementation a proof of concept so feel free to try it out
and let me know how it works for you but its mostly likely not ready for prime time.

## Attributions

If anyone is wondering, it was Richard Bowker's idea to try out Akka clustering over AMQP!

## License

akka-mqlight-remote and akka-mqlight-client are Copyright 2015 Bernard Leach and
[Licensed under the MIT license](http://opensource.org/licenses/MIT).

The TransformationApp code is released by Typesafe under Public Domain (cc0).  See
[LICENSE](https://github.com/akka/akka/blob/master/akka-samples/akka-sample-cluster-scala/LICENSE).
