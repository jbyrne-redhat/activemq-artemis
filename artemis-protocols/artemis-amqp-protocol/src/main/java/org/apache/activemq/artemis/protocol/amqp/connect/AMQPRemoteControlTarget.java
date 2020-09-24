/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.connect;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.server.remotecontrol.RemoteControl;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonAbstractReceiver;
import org.apache.activemq.artemis.utils.collections.IDSupplier;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.protocol.amqp.connect.AMQPRemoteControlsSource.EVENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.connect.AMQPRemoteControlsSource.ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.AMQPRemoteControlsSource.POST_ACK;
import static org.apache.activemq.artemis.protocol.amqp.connect.AMQPRemoteControlsSource.QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.AMQPRemoteControlsSource.ADD_ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.AMQPRemoteControlsSource.DELETE_ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.AMQPRemoteControlsSource.CREATE_QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.AMQPRemoteControlsSource.DELETE_QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.AMQPRemoteControlsSource.INTERNAL_ID;
import static org.apache.activemq.artemis.protocol.amqp.connect.AMQPRemoteControlsSource.ADDRESS_SCAN_START;
import static org.apache.activemq.artemis.protocol.amqp.connect.AMQPRemoteControlsSource.ADDRESS_SCAN_END;

public class AMQPRemoteControlTarget extends ProtonAbstractReceiver implements RemoteControl {

   public static final SimpleString INTERNAL_ID_EXTRA_PROPERTY = SimpleString.toSimpleString("x-opt-INTERNAL-ID");

   private static final Logger logger = Logger.getLogger(AMQPRemoteControlTarget.class);

   final ActiveMQServer server;

   final RoutingContextImpl routingContext = new RoutingContextImpl(null);

   Map<SimpleString, Map<SimpleString, QueueConfiguration>> scanAddresses;

   public AMQPRemoteControlTarget(AMQPSessionCallback sessionSPI,
                                  AMQPConnectionContext connection,
                                  AMQPSessionContext protonSession,
                                  Receiver receiver,
                                  ActiveMQServer server) {
      super(sessionSPI, connection, protonSession, receiver);
      new Exception("new Target").printStackTrace(System.out);
      this.server = server;
   }

   @Override
   public void flow() {
      creditRunnable.run();
   }

   @Override
   protected void actualDelivery(AMQPMessage message, Delivery delivery, Receiver receiver, Transaction tx) {
      Map<Symbol, Object> annotationsMap = message.getMessageAnnotationsMap(false);
      incrementSettle();


      System.out.println("*******************************************************************************************************************************\n" +
                         server.getIdentity() + "::Received " + message + "\n" +
                         "*******************************************************************************************************************************");
      try {
         Object eventType = annotationsMap.get(EVENT_TYPE);
         if (eventType != null) {
            // I'm not using fancy switch with strings for JDK compatibility, just in case
            if (eventType.equals(ADDRESS_SCAN_START)) {
               new Exception("Start scan").printStackTrace(System.out);
               startAddressScan();
            } else if (eventType.equals(ADDRESS_SCAN_END)) {
               new Exception("End scan").printStackTrace(System.out);
               endAddressScan();
            } else if (eventType.equals(ADD_ADDRESS)) {
               AddressInfo addressInfo = parseAddress(message);
               addAddress(addressInfo);
            } else if (eventType.equals(DELETE_ADDRESS)) {
               AddressInfo addressInfo = parseAddress(message);
               deleteAddress(addressInfo);
            } else if (eventType.equals(CREATE_QUEUE)) {
               QueueConfiguration queueConfiguration = parseQueue(message);
               createQueue(queueConfiguration);
            } else if (eventType.equals(DELETE_QUEUE)) {
               String address = (String) annotationsMap.get(ADDRESS);
               String queueName = (String) annotationsMap.get(QUEUE);
               deleteQueue(SimpleString.toSimpleString(address), SimpleString.toSimpleString(queueName));
            } else if (eventType.equals(POST_ACK)) {
               String address = (String) annotationsMap.get(ADDRESS);
               String queueName = (String) annotationsMap.get(QUEUE);
               AmqpValue value = (AmqpValue) message.getBody();
               Long messageID = (Long) value.getValue();
               postAcknowledge(address, queueName, messageID);
            }
         } else {
            sendMessage(message);
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      } finally {
         try {
            delivery.disposition(Accepted.getInstance());
            settle(delivery);
            connection.flush();
         } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
         }
      }
   }

   @Override
   public void initialize() throws Exception {
      super.initialize();
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();

      // Match the settlement mode of the remote instead of relying on the default of MIXED.
      receiver.setSenderSettleMode(receiver.getRemoteSenderSettleMode());

      // We don't currently support SECOND so enforce that the answer is anlways FIRST
      receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
      flow();
   }

   private QueueConfiguration parseQueue(AMQPMessage message) throws Exception {
      AmqpValue bodyvalue = (AmqpValue) message.getBody();
      String body = (String) bodyvalue.getValue();
      QueueConfiguration queueConfiguration = QueueConfiguration.fromJSON(body);
      return queueConfiguration;
   }

   private AddressInfo parseAddress(AMQPMessage message) throws Exception {
      AmqpValue bodyvalue = (AmqpValue) message.getBody();
      String body = (String) bodyvalue.getValue();
      AddressInfo addressInfo = AddressInfo.fromJSON(body);
      return addressInfo;
   }


   @Override
   public void startAddressScan() throws Exception {
      scanAddresses = new HashMap<>();
   }

   @Override
   public void endAddressScan() throws Exception {
      Map<SimpleString, Map<SimpleString, QueueConfiguration>> scannedAddresses = scanAddresses;
      this.scanAddresses = null;
      Stream<Binding> bindings = server.getPostOffice().getAllBindings();
      bindings.forEach((binding) -> {
         if (binding instanceof LocalQueueBinding) {
            LocalQueueBinding localQueueBinding = (LocalQueueBinding) binding;
            Map<SimpleString, QueueConfiguration> scannedQueues = scannedAddresses.get(localQueueBinding.getQueue().getAddress());

            if (scannedQueues == null) {
               System.out.println("There's no address " + localQueueBinding.getQueue().getAddress() + " so, removing queue");
               try {
                  deleteQueue(localQueueBinding.getQueue().getAddress(), localQueueBinding.getQueue().getName());
               } catch (Exception e) {
                  logger.warn(e.getMessage(), e);
               }
            } else {
               QueueConfiguration queueConfg = scannedQueues.get(localQueueBinding.getQueue().getName());
               if (queueConfg == null) {
                  System.out.println("There no queue for " + localQueueBinding.getQueue().getName() + " so, removing queue");
                  try {
                     deleteQueue(localQueueBinding.getQueue().getAddress(), localQueueBinding.getQueue().getName());
                  } catch (Exception e) {
                     logger.warn(e.getMessage(), e);
                  }
               }
            }
         }
      });
   }

   private Map<SimpleString, QueueConfiguration> getQueueScanMap(SimpleString address) {
      Map<SimpleString, QueueConfiguration> queueMap = scanAddresses.get(address);
      if (queueMap == null) {
         queueMap = new HashMap<>();
         scanAddresses.put(address, queueMap);
      }
      return queueMap;
   }

   @Override
   public void addAddress(AddressInfo addressInfo) throws Exception {
      System.out.println("*******************************************************************************************************************************");
      System.out.println("Adding address on the other side..." + addressInfo);
      server.addAddressInfo(addressInfo);
   }

   @Override
   public void deleteAddress(AddressInfo addressInfo) throws Exception {
      System.out.println("*******************************************************************************************************************************");
      System.out.println("delete address " + addressInfo);
      try {
         server.removeAddressInfo(addressInfo.getName(), null, true);
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

   @Override
   public void createQueue(QueueConfiguration queueConfiguration) throws Exception {
      System.out.println("*******************************************************************************************************************************");
      System.out.println("Adding queue " + queueConfiguration);
      server.createQueue(queueConfiguration, true);

      if (scanAddresses != null) {
         getQueueScanMap(queueConfiguration.getAddress()).put(queueConfiguration.getName(), queueConfiguration);
      }
   }

   @Override
   public void deleteQueue(SimpleString addressName, SimpleString queueName) throws Exception {
      System.out.println("*******************************************************************************************************************************");
      System.out.println("destroy queue " + queueName);
      server.destroyQueue(queueName);
   }

   @Override
   public void routingDone(List<MessageReference> refs, boolean direct) {

   }

   private static IDSupplier<MessageReference> referenceIDSupplier = new IDSupplier<MessageReference>() {
      @Override
      public Object getID(MessageReference source) {
         Long id = (Long) source.getMessage().getBrokerProperty(INTERNAL_ID_EXTRA_PROPERTY);
         return id;
      }
   };

   public void postAcknowledge(String address, String queue, long messageID) {
      if (logger.isTraceEnabled()) {
         logger.trace("post acking " + address + ", queue = " + queue + ", messageID = " + messageID);
      }

      Queue targetQueue = server.locateQueue(queue);
      if (targetQueue != null) {
         MessageReference reference = targetQueue.removeWithSuppliedID(messageID, referenceIDSupplier);
         if (reference != null) {
            if (logger.isDebugEnabled()) {
               logger.debug("Acking reference " + reference);
            }
            try {
               targetQueue.acknowledge(reference);
            } catch (Exception e) {
               // TODO anything else I can do here?
               // such as close the connection with error?
               logger.warn(e.getMessage(), e);
            }
         } else {
            if (logger.isTraceEnabled()) {
               logger.trace("There is no reference to ack on " + messageID);
            }
         }
      }

   }

   private void sendMessage(AMQPMessage message) throws Exception {
      if (message.getMessageID() <= 0) {
         message.setMessageID(server.getStorageManager().generateID());
      }

      DeliveryAnnotations deliveryAnnotations = message.getDeliveryAnnotations();

      if (deliveryAnnotations != null) {
         Long internalID = (Long) deliveryAnnotations.getValue().get(INTERNAL_ID);
         if (internalID != null) {
            message.setBrokerProperty(INTERNAL_ID_EXTRA_PROPERTY, internalID);
         }
      }

      routingContext.clear();
      server.getPostOffice().route(message, routingContext, false);
      flow();
   }

   /**
    * not implemented on the target, treated at {@link #postAcknowledge(String, String, long)}
    *
    * @param ref
    * @param reason
    */
   @Override
   public void postAcknowledge(MessageReference ref, AckReason reason) {
   }

   /**
    * not implemented on the target, treated at {@link #sendMessage(AMQPMessage)}
    *
    * @param message
    * @param context
    * @param refs
    */
   @Override
   public void sendMessage(Message message, RoutingContext context, List<MessageReference> refs) {
   }

}
