/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.protocol.amqp.connect;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerQueuePlugin;
import org.apache.activemq.artemis.core.server.remotecontrol.RemoteControl;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPIllegalStateException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolLogger;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext;
import org.apache.activemq.artemis.protocol.amqp.proton.SenderInitializer;
import org.apache.activemq.artemis.protocol.amqp.sasl.ClientSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.ClientSASLFactory;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.remoting.ClientConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.jboss.logging.Logger;

public class AMQPOutgoingConnection implements ClientConnectionLifeCycleListener, ActiveMQServerQueuePlugin {

   private static final Logger logger = Logger.getLogger(AMQPOutgoingConnection.class);

   private final AMQPBrokerConnectConfiguration amqpConfiguration;
   private final ProtonProtocolManager protonProtocolManager;
   private final ActiveMQServer server;
   private final NettyConnector bridgesConnector;
   private NettyConnection connection;
   private Session session;
   AMQPSessionContext sessionContext;
   ActiveMQProtonRemotingConnection protonRemotingConnection;
   private volatile boolean started = false;
   private final AMQPOutgoingConnectionManager bridgeManager;
   private int retryCounter = 0;
   private volatile ScheduledFuture reconnectFuture;
   Map<Queue, Sender> senders = new HashMap<>();
   Map<Queue, Receiver> receivers = new HashMap<>();

   final Executor connectExecutor;
   final ScheduledExecutorService scheduledExecutorService;

   /** This is just for logging.
    *  the actual connection will come from the amqpConnection configuration*/
   String host;

   /** This is just for logging.
    *  the actual connection will come from the amqpConnection configuration*/
   int port;

   public AMQPOutgoingConnection(AMQPOutgoingConnectionManager bridgeManager, AMQPBrokerConnectConfiguration amqpConfiguration,
                                 ProtonProtocolManager protonProtocolManager,
                                 ActiveMQServer server,
                                 NettyConnector bridgesConnector) {
      this.bridgeManager = bridgeManager;
      this.amqpConfiguration = amqpConfiguration;
      this.protonProtocolManager = protonProtocolManager;
      this.server = server;
      this.bridgesConnector = bridgesConnector;
      connectExecutor = server.getExecutorFactory().getExecutor();
      scheduledExecutorService = server.getScheduledPool();
   }

   public void stop() {
      if (connection != null) {
         connection.close();
      }
      ScheduledFuture scheduledFuture = reconnectFuture;
      reconnectFuture = null;
      if (scheduledFuture != null) {
         scheduledFuture.cancel(true);
      }
      started = false;
   }

   public void connect() throws Exception {
      started = true;
      server.getConfiguration().registerBrokerPlugin(this);
      try {

         for (AMQPBrokerConnectionElement connectionElement : amqpConfiguration.getConnectionElements()) {
            if (connectionElement.getType() == AMQPBrokerConnectionAddressType.mirror) {
               installRemoteControl((AMQPMirrorBrokerConnectionElement)connectionElement, server);
            }
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
         return;
      }
      connectExecutor.execute(() -> doConnect());
   }

   public NettyConnection getConnection() {
      return connection;
   }

   @Override
   public void afterCreateQueue(Queue queue) {
      connectExecutor.execute(() -> {
         for (AMQPBrokerConnectionElement connectionElement : amqpConfiguration.getConnectionElements()) {
            validateMatching(queue, connectionElement);
         }
      });
   }

   public void validateMatching(Queue queue, AMQPBrokerConnectionElement connectionElement) {
      if (connectionElement.getType() != AMQPBrokerConnectionAddressType.mirror) {
         if (connectionElement.getQueueName() != null) {
            if (queue.getName().equals(connectionElement.getQueueName())) {
               createLink(queue, connectionElement);
            }
         } else if (connectionElement.match(queue.getAddress(), server.getConfiguration().getWildcardConfiguration())) {
            createLink(queue, connectionElement);
         }
      }
   }

   public void createLink(Queue queue, AMQPBrokerConnectionElement connectionElement) {
      if (connectionElement.getType() == AMQPBrokerConnectionAddressType.peer) {
         connectSender(false, queue, queue.getAddress().toString(), Symbol.valueOf("qd.waypoint"));
         connectReceiver(protonRemotingConnection, session, sessionContext, queue, Symbol.valueOf("qd.waypoint"));
      } else {
         if (connectionElement.getType() == AMQPBrokerConnectionAddressType.sender) {
            connectSender(false, queue, queue.getAddress().toString());
         }
         if (connectionElement.getType() == AMQPBrokerConnectionAddressType.receiver) {
            connectReceiver(protonRemotingConnection, session, sessionContext, queue);
         }
      }
   }

   private void doConnect() {
      try {
         List<TransportConfiguration> configurationList = amqpConfiguration.getTransportConfigurations();

         TransportConfiguration tpConfig = configurationList.get(0);

         String hostOnParameter = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST, tpConfig.getParams());
         int portOnParameter = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT, tpConfig.getParams());
         this.host = hostOnParameter;
         this.port = portOnParameter;
         connection = bridgesConnector.createConnection(null, hostOnParameter, portOnParameter);

         if (connection == null) {
            retryConnection();
            return;
         }

         reconnectFuture = null;
         retryCounter = 0;

         // before we retry the connection we need to remove any previous links
         // as they will need to be recreated
         senders.clear();
         receivers.clear();

         ClientSASLFactory saslFactory = null;

         if (amqpConfiguration.getUser() != null && amqpConfiguration.getPassword() != null) {
            saslFactory = availableMechanims -> {
               if (availableMechanims != null && Arrays.asList(availableMechanims).contains("PLAIN")) {
                  return new PlainSASLMechanism(amqpConfiguration.getUser(), amqpConfiguration.getPassword());
               } else {
                  return null;
               }
            };
         }

         ConnectionEntry entry = protonProtocolManager.createOutgoingConnectionEntry(connection, saslFactory);
         protonRemotingConnection = (ActiveMQProtonRemotingConnection) entry.connection;
         connection.getChannel().pipeline().addLast(new AMQPOutgoingChannelHandler(bridgesConnector.getChannelGroup(), protonRemotingConnection.getAmqpConnection().getHandler()));

         protonRemotingConnection.getAmqpConnection().runLater(() -> {
            protonRemotingConnection.getAmqpConnection().open();
            protonRemotingConnection.getAmqpConnection().flush();
         });

         session = protonRemotingConnection.getAmqpConnection().getHandler().getConnection().session();
         sessionContext = protonRemotingConnection.getAmqpConnection().getSessionExtension(session);
         protonRemotingConnection.getAmqpConnection().runLater(() -> {
            session.open();
            protonRemotingConnection.getAmqpConnection().flush();
         });

         if (amqpConfiguration.getConnectionElements() != null) {
            Stream<Binding> bindingStream = server.getPostOffice().getAllBindings();

            bindingStream.forEach(binding -> {
               if (binding instanceof QueueBinding) {
                  Queue queue = ((QueueBinding) binding).getQueue();
                  for (AMQPBrokerConnectionElement connectionElement : amqpConfiguration.getConnectionElements()) {
                     validateMatching(queue, connectionElement);
                  }
               }
            });

            for (AMQPBrokerConnectionElement connectionElement : amqpConfiguration.getConnectionElements()) {
               if (connectionElement.getType() == AMQPBrokerConnectionAddressType.mirror) {
                  AMQPMirrorBrokerConnectionElement replica = (AMQPMirrorBrokerConnectionElement)connectionElement;
                  Queue queue = server.locateQueue(replica.getSourceMirrorAddress());

                  connectSender(true, queue, protonProtocolManager.getMirrorAddress());
               }
            }
         }

         protonRemotingConnection.getAmqpConnection().flush();

         bridgeManager.connected(connection, this);
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
         redoConnection();
      }
   }

   public void retryConnection() {
      if (bridgeManager.isStarted() && started) {
         if (amqpConfiguration.getReconnectAttempts() < 0 || retryCounter < amqpConfiguration.getReconnectAttempts()) {
            retryCounter++;
            ActiveMQAMQPProtocolLogger.LOGGER.retryConnection(amqpConfiguration.getName(), host, port, retryCounter, amqpConfiguration.getReconnectAttempts());
            if (logger.isDebugEnabled()) {
               logger.debug("Reconnecting in " + amqpConfiguration.getRetryInterval() + ", this is the " + retryCounter + " of " + amqpConfiguration.getReconnectAttempts());
            }
            reconnectFuture = scheduledExecutorService.schedule(() -> connectExecutor.execute(() -> doConnect()), amqpConfiguration.getRetryInterval(), TimeUnit.MILLISECONDS);
         } else {
            ActiveMQAMQPProtocolLogger.LOGGER.retryConnectionFailed(amqpConfiguration.getName(), host, port, retryCounter, amqpConfiguration.getReconnectAttempts());
            if (logger.isDebugEnabled()) {
               logger.debug("no more reconnections as the retry counter reached " + retryCounter + " out of " + amqpConfiguration.getReconnectAttempts());
            }
         }
      }
   }

   /** The reason this method is static is the following:
    *
    *  It is returning the snfQueue to the replica, and I needed isolation from the actual instance.
    *  During development I had a mistake where I used a property from the Object,
    *  so, I needed this isolation for my organization and making sure nothing would be shared. */
   private static QueueBinding installRemoteControl(AMQPMirrorBrokerConnectionElement replicaConfig, ActiveMQServer server) throws Exception {

      AddressInfo addressInfo = server.getAddressInfo(replicaConfig.getSourceMirrorAddress());
      if (addressInfo == null) {
         addressInfo = new AddressInfo(replicaConfig.getSourceMirrorAddress()).addRoutingType(RoutingType.ANYCAST).setAutoCreated(false).setTemporary(!replicaConfig.isDurable());
         server.addAddressInfo(addressInfo);
      }

      if (addressInfo.getRoutingType() != RoutingType.ANYCAST) {
         throw new IllegalArgumentException("sourceMirrorAddress is not ANYCAST");
      }

      Queue remoteControlQueue = server.locateQueue(replicaConfig.getSourceMirrorAddress());

      if (remoteControlQueue == null) {
         remoteControlQueue = server.createQueue(new QueueConfiguration(replicaConfig.getSourceMirrorAddress()).setAddress(replicaConfig.getSourceMirrorAddress()).setRoutingType(RoutingType.ANYCAST).setDurable(replicaConfig.isDurable()), true);
      }

      remoteControlQueue.setRemoteControl(true);

      QueueBinding snfReplicaQueueBinding = (QueueBinding)server.getPostOffice().getBinding(replicaConfig.getSourceMirrorAddress());
      if (snfReplicaQueueBinding == null) {
         logger.warn("Queue does not exist even after creation! " + replicaConfig);
         throw new IllegalAccessException("Cannot start replica");
      }

      Queue snfQueue = snfReplicaQueueBinding.getQueue();

      if (!snfQueue.getAddress().equals(replicaConfig.getSourceMirrorAddress())) {
         logger.warn("Queue " + snfQueue + " belong to a different address (" + snfQueue.getAddress() + "), while we expected it to be " + addressInfo.getName());
         throw new IllegalAccessException("Cannot start replica");
      }

      AMQPRemoteControlsSource newPartition = new AMQPRemoteControlsSource(snfQueue, server, replicaConfig.isMessageAcknowledgements());

      server.scanAddresses(newPartition);

      RemoteControl currentRemoteControl = server.getRemoteControl();

      if (currentRemoteControl == null) {
         server.installRemoteControl(newPartition);
      } else {
         // Replace a standard implementation by an aggregated supporting multiple targets
         if (currentRemoteControl instanceof AMQPRemoteControlsSource) {
            // replacing the simple remote control for an aggregator
            AMQPRemoteControlsAggregation remoteAggregation = new AMQPRemoteControlsAggregation();
            remoteAggregation.addPartition((AMQPRemoteControlsSource)currentRemoteControl);
            currentRemoteControl = remoteAggregation;
            server.installRemoteControl(remoteAggregation);
         }
         ((AMQPRemoteControlsAggregation)currentRemoteControl).addPartition(newPartition);
      }

      return snfReplicaQueueBinding;
   }

   private void connectReceiver(ActiveMQProtonRemotingConnection protonRemotingConnection,
                                Session session,
                                AMQPSessionContext sessionContext,
                                Queue queue,
                                Symbol... capabilities) {
      if (logger.isDebugEnabled()) {
         logger.debug("Connecting inbound for " + queue);
      }
      {
         Receiver checkRec = receivers.get(queue);

         if (checkRec != null) {
            logger.debug("Receiver for queue " + queue + " already exists, just giving up");
            return;
         }
      }

      if (session == null) {
         logger.debug("session is null");
         return;
      }

      Receiver receiver = session.receiver(queue.getName().toString() + UUIDGenerator.getInstance().generateStringUUID());
      Target target = new Target();
      target.setAddress(queue.getAddress().toString());
      receiver.setTarget(target);

      Source source = new Source();
      source.setAddress(queue.getName().toString());
      receiver.setSource(source);


      if (capabilities != null) {
         source.setCapabilities(capabilities);
      }

      receivers.put(queue, receiver);

      protonRemotingConnection.getAmqpConnection().runLater(() -> {
         receiver.open();
         protonRemotingConnection.getAmqpConnection().flush();
         try {
            sessionContext.addReceiver(receiver);
         } catch (Exception e) {
            error(e);
         }
      });
   }

   private void connectSender(boolean remoteControl,
                                Queue queue,
                                String targetName,
                                Symbol... capabilities) {
      // TODO: Adding log.debug here
      if (logger.isDebugEnabled()) {
         logger.debug("Connecting outbound for " + queue);
      }

      {
         Sender sender = senders.get(queue);
         if (sender != null) {
            logger.debug("Sender for queue " + queue + " already exists, just giving up");
            return;
         }
      }

      if (session == null) {
         logger.debug("Session is null");
         return;
      }

      Sender sender = session.sender(targetName + UUIDGenerator.getInstance().generateStringUUID());
      Target target = new Target();
      target.setAddress(targetName);
      if (capabilities != null) {
         target.setCapabilities(capabilities);
      }
      sender.setTarget(target);

      Source source = new Source();
      source.setAddress(queue.getName().toString());
      sender.setSource(source);

      AMQPOutgoingInitializer outgoingInitializer = new AMQPOutgoingInitializer(queue, sender, sessionContext.getSessionSPI());


      ProtonServerSenderContext senderContext;
      if (remoteControl) {
         senderContext = new RemoteControlServerSenderContext(protonRemotingConnection.getAmqpConnection(), sender, sessionContext, sessionContext.getSessionSPI(), outgoingInitializer);
      } else {
         senderContext = new ProtonServerSenderContext(protonRemotingConnection.getAmqpConnection(), sender, sessionContext, sessionContext.getSessionSPI(), outgoingInitializer);
      }

      senders.put(queue, sender);

      protonRemotingConnection.getAmqpConnection().runLater(() -> {
         try {
            sessionContext.addSender(sender, senderContext);
         } catch (Exception e) {
            error(e);
         }
         protonRemotingConnection.getAmqpConnection().flush();
      });
   }

   static class RemoteControlServerSenderContext extends ProtonServerSenderContext {

      RemoteControlServerSenderContext(AMQPConnectionContext connection,
                                                 Sender sender,
                                                 AMQPSessionContext protonSession,
                                                 AMQPSessionCallback server,
                                                 SenderInitializer senderInitializer) {
         super(connection, sender, protonSession, server, senderInitializer);
      }

      @Override
      protected void doAck(Message message) throws ActiveMQAMQPIllegalStateException {
         super.doAck(message);
      }
   }

   protected void error(Throwable e) {
      e.printStackTrace();
      // TODO: Make this async
      //       and make this retry
   }

   private class AMQPOutgoingInitializer implements SenderInitializer {

      final Queue queue;
      final Sender sender;
      final AMQPSessionCallback sessionSPI;

      AMQPOutgoingInitializer(Queue queue, Sender sender, AMQPSessionCallback sessionSPI) {
         this.queue = queue;
         this.sessionSPI = sessionSPI;
         this.sender = sender;
      }

      @Override
      public Consumer init(ProtonServerSenderContext senderContext) throws Exception {
         SimpleString queueName = queue.getName();
         return (Consumer) sessionSPI.createSender(senderContext, queueName, null, false);
      }

      @Override
      public void close() throws Exception {
         // TODO implement close
      }
   }

   public void disconnect() throws Exception {
      redoConnection();
   }

   @Override
   public void connectionCreated(ActiveMQComponent component, Connection connection, ClientProtocolManager protocol) {
   }

   @Override
   public void connectionDestroyed(Object connectionID) {
      redoConnection();
   }

   @Override
   public void connectionException(Object connectionID, ActiveMQException me) {
      redoConnection();
   }

   private void redoConnection() {
      try {
         if (connection != null) {
            connection.close();
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      }

      retryConnection();

   }

   @Override
   public void connectionReadyForWrites(Object connectionID, boolean ready) {
      protonRemotingConnection.flush();
   }

   private static class PlainSASLMechanism implements ClientSASL {

      private final byte[] initialResponse;

      PlainSASLMechanism(String username, String password) {
         byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
         byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
         byte[] encoded = new byte[usernameBytes.length + passwordBytes.length + 2];
         System.arraycopy(usernameBytes, 0, encoded, 1, usernameBytes.length);
         System.arraycopy(passwordBytes, 0, encoded, usernameBytes.length + 2, passwordBytes.length);
         initialResponse = encoded;
      }

      @Override
      public String getName() {
         return "PLAIN";
      }

      @Override
      public byte[] getInitialResponse() {
         return initialResponse;
      }

      @Override
      public byte[] getResponse(byte[] challenge) {
         return new byte[0];
      }
   }

}
