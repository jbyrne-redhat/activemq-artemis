# Broker Connections
[//]: <> (This line is commnented out from the output: In case we add other protocol types on server connections, this chapter will need to renamed and some context changed )

Server connections were introduced in ActiveMQ Artemis at version 2.16.0. At the time of the implementation we introduced the capability of Broker Connections.

Instead of waiting for clients to connect, the ActiveMQ Artemis Broker will be proactive and create a connection to another endpoint on a specific protocol without.

At the moment this feature the only supporting protocol is AMQP however it could be eventually expanded to other protocols.

Server connections are configured through `broker.xml`

```xml
<broker-connections>
    ...
</broker-connections>
```

# AMQP Server Connections
ActiveMQ Artemis has the capability of initiating a connection towards other endpoints using the AMQP Protocol.

That means the broker will connect to another AMQP server (not necessarily ActiveMQ Artemis) and create elements towards that connection.

You can define an amqp broker connection with the XML element amqp-connection, with the following arguments

- uri : tcp://host:myport (this is a required argument)
- name : The name of the connection used for management purposes
- user : you can optionally specify the user to connect on the endpoint. 
- password: you can optionally specify the password to connect on the endpoint
- retry-interval: how long to wait in milliseconds before retrying a connection after an error. (default: 5000)
- reconnect-attempts: default is -1 meaning infinite

This is an example of an amqp broker connection definition:

```xml
<broker-connections>
    <amqp-connection uri="tcp://MY_HOST:MY_PORT" name="my-broker" retry-interval="100" reconnect-attempts="-1" user="john" password="doe">
        <mirror  queue-removal="true" target-mirror-address="$mirror" queue-creation="false" message-acknowledgements="true" source-mirror-address=""/>
        <peer match="queues.#"/>
    </amqp-connection>
</broker-connections>
```
*Important:* The target endpoint needs permission on all operations you configure. So if you're using a security manager make sure you use an user with enough privileges to perform your configured operations.

# AMQP Server Connection elements
We currently support the following types of elements on a AMQP Server Connection:

- Senders
    - Messages received on specific queues will be transferred to another endpoint
- Receivers
    - The broker will pull messages from another endpoint
- Peers
    - The broker will create both senders and receivers to another endpoint that knows how to handle it. This is currently implement by qpid-dispatch.
- Mirrors
    - The broker will use an AMQP connection towards another broker and duplicate messages and send acknowledgements over the wire.
     
This is a snippet on how you configure an AMQP Server Connection:

```xml
<broker-connections>
 <amqp-connection uri="tcp://MY_HOST:MY_PORT" name="my-broker">
     ... amqp lements ...
 </amqp-connection>
</broker-connections>
```

## Senders and Receivers
It is possible to connect an ActiveMQ Artemis Broker towards another AMQP endpoint, by simply creating a sender or receiver broker connection element.

For a `sender`, the broker will create a message consumer on a queue, that will *send* messages towards another AMQP endpoint.

For a `receiver`, the broker will create a message producer on an address, that will *receive* messages from another AMQP endpoint.

Both elements will work like a message bridge however without any extra weight required to process messages. They are just like any other consumer or producer as part of ActiveMQ Artemis Broker.

Senders and Receivers can be specified by address matching using [wildcard expressions](wildcard-syntax.md).

There are two properties you can use to configure a sender or receiver:

- match: for the wildcard expression mathing addresses
- queue-name: The name of a queue, without expressions

Example of configuring senders and receivers using address expressions:

```xml
<broker-connections>
 <amqp-connection uri="tcp://MY_HOST:MY_PORT" name="my-broker">
    <sender match="queues.#"/>
    <!-- notice the local queues for removequeues.# need to be created on this broker -->
    <sender match="remotequeues.#"/>
 </amqp-connection>
</broker-connections>

<addresses>
 <address name="remotequeues.A">
    <anycast>
       <queue name="remoteQueueA"/>
    </anycast>
 </address>
 <address name="queues.B">
    <anycast>
       <queue name="localQueueB"/>
    </anycast>
 </address>
</addresses>
```

Example of configurig senders and receivers using queue names:

```xml
<broker-connections>
    <amqp-connection uri="tcp://MY_HOST:MY_PORT" name="my-broker">
        <sender queue-name="remoteQueueA"/>
        <sender queue-name="localQueueB"/>
    </amqp-connection>
</broker-connections>

<addresses>
     <address name="remotequeues.A">
        <anycast>
           <queue name="remoteQueueA"/>
        </anycast>
     </address>
     <address name="queues.B">
        <anycast>
           <queue name="localQueueB"/>
        </anycast>
     </address>
</addresses>

```
*Important:* Notice that on the `receiver` case we will only match local existent queues, so if you are using receivers make sure you pre-create the queue locally otherwise we broker will not be able to match remote queues and addresses.

*Important:* Be careful to not create a sender and a receiver to the same destination, otherwise they will incur in infinite echoes of sends and receives.


# Peers
A peer broker connection element will be a mix of sender and receivers. The ActiveMQ Artemis broker will create both a sender and a receiver for a peer element, and the endpoint should know how to deal with the pair without creating an infinite feedback of sending and receiving messages.

Currently [Apache QPID Dispatch Router](https://qpid.apache.org/components/dispatch-router/index.html) as a peer. ActiveMQ Artemis will create the pair of receivers and sender for each matching destination, and these senders and receivers will have special configuration to let qpid dispatch know to collaborate with an ActiveMQ Artemis Broker.

You would be able to play with advanced networking scenarios with Apache QPID Dispatch Router and getting a lot of benefit out of the AMQP protocol and its eco system.

With a peer, you have the same properties you would have on a sender and receiver. So the configuration is fairly similar to a sender or a receiver:

```xml
<broker-connections>
    <amqp-connection uri="tcp://MY_HOST:MY_PORT" name="my-broker">
       <peer match="queues.#"/>
    </amqp-connection>
</broker-connections>

<addresses>
     <address name="queues.A">
        <anycast>
           <queue name="localQueueA"/>
        </anycast>
     </address>
     <address name="queues.B">
     <anycast>
        <queue name="localQueueB"/>
     </anycast>
    </address>
</addresses>
```

*Important:* Do not use this feature to connect to another broker, otherwise any message send will be immediately ready to consume creating an infinite echo of sends and receives.

# Mirror (Disaster, Recovery and HA)
We will discuss Mirror more in detail on the [next chapter](amqp-server-DR.md). As for now, lets just focus on the configuration aspect of a broker connection that contains a mirror element.

All you have to do is to define the mirror element, where you have these optional arguments:

## Boolean arguments:
- queue-removal = true | false (default true)
Should queue and address removes to be send through the wire.

- message-acknowledgements = true | false (default true)
Should message acknowledgements to be sent through the wire.

- queue-creation = true | false (default true)
Should queue and address creation to be sent through the wire.

## String arguments:
- target-mirror-address (default: $mirror)
The AMQP mirror option will create a sender targetting an address with the name defined here. The AMQP Protocol Layer will treat the special messages sent through Mirroring when the target matches this name. You can change the target name on the acceptor with the acceptor property mirrorAddress.

- source-mirror-address (default: empty)
By default the AMQP Mirror Option will create a non durable temporary queue to stores messages before they are sent towards the other broker.
If you define this property an ANYCAST durable queue and address will be created with this name.

This is an option where we fully define an AMQP Broker Connection with a mirror option:

```xml
<broker-connections>
    <amqp-connection uri="tcp://MY_HOST:MY_PORT" name="my-broker">
            <mirror  queue-removal="true" target-mirror-address="brokerMirror" queue-creation="true" message-acknowledgements="true" source-mirror-address="myLocalSNFMirrorQueue"/>
    </amqp-connection>
</broker-connections>
```

On the activemq broker accepting the mirror connection (the replica), the acceptor will need to match the target-mirror-address (if you choose to specify your own name):

```xml
<acceptors>
   <acceptor name="artemis">tcp://MY_HOST:MY_PORT?brokerMirror;tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;amqpMinLargeMessageSize=102400;protocols=CORE,AMQP,STOMP,HORNETQ,MQTT,OPENWIRE;useEpoll=true;amqpCredits=1000;amqpLowCredits=300;amqpDuplicateDetection=true</acceptor>
</acceptors>

```

*Important:* The reason the mirror address is configurable, is that within AMQP you can use special network configuration (through Apache Qpid Dispatch for example) where you have to specify the address of a sender, and define a network route between your host and the replica datacenter. If you can leave, use default values for `target-mirror-address`.

