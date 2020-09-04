/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerMessagePlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test broker behavior when creating AMQP senders
 */
public class AmqpReferenceDeliveryAnnotationTest extends AmqpClientTestSupport {

   @Override
   protected void addAdditionalAcceptors(ActiveMQServer server) throws Exception {
   }

   @Test
   public void testReceiveAnnotations() throws Exception {
      internalReceiveAnnotations(false);
   }

   @Test
   public void testReceiveAnnotationsLargeMessage() throws Exception {
      internalReceiveAnnotations(true);
   }

   public void internalReceiveAnnotations(boolean largeMessage) throws Exception {

      server.getConfiguration().registerBrokerPlugin(new ActiveMQServerMessagePlugin() {

         public void beforeDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
            Map<Symbol, Object> symbolObjectMap = new HashMap<>();
            DeliveryAnnotations deliveryAnnotations = new DeliveryAnnotations(symbolObjectMap);
            symbolObjectMap.put(Symbol.getSymbol("HELLO"), "WORLD");
            reference.setProtocolData(deliveryAnnotations);
         }
      });

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      AmqpMessage message = new AmqpMessage();

      String body;
      if (largeMessage) {
         StringBuffer buffer = new StringBuffer();
         for (int i = 0 ; i < 1024 * 1024; i++) {
            buffer.append("*");
         }
         body = buffer.toString();
      } else {
         body = "test-message";
      }

      message.setMessageId("msg" + 1);
      message.setText(body);
      sender.send(message);

      message = new AmqpMessage();
      message.setMessageId("msg" + 2);
      message.setText(body);
      sender.send(message);
      sender.close();

      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(2);
      AmqpMessage received = receiver.receive(10, TimeUnit.SECONDS);
      assertNotNull("Should have read message", received);
      assertEquals("msg1", received.getMessageId());
      assertEquals("WORLD", received.getDeliveryAnnotation("HELLO"));
      received.accept();

      received = receiver.receive(10, TimeUnit.SECONDS);
      assertNotNull("Should have read message", received);
      assertEquals("msg2", received.getMessageId());
      assertEquals("WORLD", received.getDeliveryAnnotation("HELLO"));
      received.accept();

      Assert.assertNull(receiver.receiveNoWait());

      receiver.close();

      connection.close();
   }
}