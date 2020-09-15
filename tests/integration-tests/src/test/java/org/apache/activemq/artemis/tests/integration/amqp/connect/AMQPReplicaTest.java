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

package org.apache.activemq.artemis.tests.integration.amqp.connect;


import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.artemis.core.config.amqpbridging.AMQPConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpbridging.AMQPReplica;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class AMQPReplicaTest extends AmqpClientTestSupport {

   protected static final int AMQP_PORT_2 = 5673;
   protected static final int AMQP_PORT_3 = 5674;

   ActiveMQServer server_2;

   @Ignore // not implemented yet
   @Test
   public void testReplicaWithPull() throws Exception {
   }

   @Ignore // not implemented yet
   @Test
   public void testReplicaDisconnect() throws Exception {
   }

   @Ignore // not implemented yet
   @Test
   public void testReplicaCatchupOnQueueCreates() throws Exception {

   }
   @Test
   public void testReplicaWithPushLargeMessages() throws Exception {
      replicaTest(true, true, true, false, false);
   }

   @Test
   public void testReplicaWithPushLargeMessagesPagingEverywhere() throws Exception {
      replicaTest(true, true, true, true, true);
   }
   @Test
   public void testReplicaWithPush() throws Exception {
      replicaTest(true, false, true, false, false);
   }

   @Test
   public void testReplicaWithPushPagedTarget() throws Exception {
      replicaTest(true, false, true, true, false);
   }

   @Test
   public void testReplicaWithPushPagingEverywhere() throws Exception {
      replicaTest(true, false, true, true, true);
   }

   private String getText(boolean large, int i) {
      if (!large) {
         return "Text " + i;
      } else {
         StringBuffer buffer = new StringBuffer();
         while (buffer.length() < 110 * 1024) {
            buffer.append("Text " + i + " ");
         }
         return buffer.toString();
      }
   }

   private void replicaTest(boolean push, boolean largeMessage, boolean acks, boolean pagingTarget, boolean pagingSource) throws Exception {
      server.setIdentity("targetServer");
      //server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("TEST"), RoutingType.ANYCAST));
      //server.createQueue(new QueueConfiguration("TEST").setRoutingType(RoutingType.ANYCAST));

      /** if (!push) { -- TODO configure this on the server, not here though!
       AMQPConnectConfiguration amqpConnection = new AMQPConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT_2);
       amqpConnection.setReplica(new AMQPReplica("REPLICA", "SERVER2", false));
       } */

      server_2 = createServer(AMQP_PORT_2, false);

      if (push) {
         AMQPConnectConfiguration amqpConnection = new AMQPConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
         amqpConnection.setReplica(new AMQPReplica("SNFREPLICA", true));
         server_2.getConfiguration().addAMQPConnection(amqpConnection);
      }

      int NUMBER_OF_MESSAGES = 100;

      server_2.start();
      Wait.assertTrue(server_2::isStarted);

      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT_2);
      Connection connection = factory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(session.createQueue("TEST"));
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      Queue queueOnServer1 = locateQueue(server, "TEST");

      if (pagingTarget) {
         queueOnServer1.getPagingStore().startPaging();
      }

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         Message message = session.createTextMessage(getText(largeMessage, i));
         message.setIntProperty("i", i);
         producer.send(message);
      }


      Wait.assertEquals(NUMBER_OF_MESSAGES, queueOnServer1::getMessageCount);

      if (pagingTarget) {
         assertTrue(queueOnServer1.getPagingStore().isPaging());
      }

      if (acks) {
         consumeMessages(largeMessage, 0, NUMBER_OF_MESSAGES / 2 - 1, AMQP_PORT_2, false);
         // Replica is async, so we need to wait acks to arrive before we finish consuming there
         Wait.assertEquals(NUMBER_OF_MESSAGES / 2, queueOnServer1::getMessageCount);
         consumeMessages(largeMessage, NUMBER_OF_MESSAGES / 2, NUMBER_OF_MESSAGES - 1, AMQP_PORT, true); // We consume on both servers as this is currently replicated
      }
   }

   @Test
   public void testDualStandard() throws Exception {
      dualReplica(false, false, false);
   }

   @Test
   public void testDualRegularPagedTargets() throws Exception {
      dualReplica(false, false, true);
   }

   @Test
   public void testDualRegularPagedEverything() throws Exception {
      dualReplica(false, true, true);
   }

   @Test
   public void testDualRegularLarge() throws Exception {
      dualReplica(true, false, false);
   }

   public Queue locateQueue(ActiveMQServer server, String queueName) throws Exception {
      Wait.waitFor(() -> server.locateQueue(queueName) != null);
      return server.locateQueue(queueName);
   }


   private void dualReplica(boolean largeMessage, boolean pagingSource, boolean pagingTarget) throws Exception {
      server.setIdentity("server_1");
      //server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("TEST"), RoutingType.ANYCAST));
      //server.createQueue(new QueueConfiguration("TEST").setRoutingType(RoutingType.ANYCAST));

      /** if (!push) { -- TODO configure this on the server, not here though!
       AMQPConnectConfiguration amqpConnection = new AMQPConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT_2);
       amqpConnection.setReplica(new AMQPReplica("REPLICA", "SERVER2", false));
       } */

      ActiveMQServer server_3 = createServer(AMQP_PORT_3, false);
      server_3.setIdentity("server_3");
      server_3.start();
      Wait.assertTrue(server_3::isStarted);

      ConnectionFactory factory_3 = CFUtil.createConnectionFactory("amqp", "tcp://localhost:" + AMQP_PORT_3);
      factory_3.createConnection().close();

      server_2 = createServer(AMQP_PORT_2, false);

      AMQPConnectConfiguration amqpConnection1 = new AMQPConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
      amqpConnection1.setReplica(new AMQPReplica("REPLICA1", true));
      server_2.getConfiguration().addAMQPConnection(amqpConnection1);

      AMQPConnectConfiguration amqpConnection3 = new AMQPConnectConfiguration("test2", "tcp://localhost:" + AMQP_PORT_3);
      amqpConnection3.setReplica(new AMQPReplica("REPLICA2", true));
      server_2.getConfiguration().addAMQPConnection(amqpConnection3);

      int NUMBER_OF_MESSAGES = 200;

      server_2.start();
      Wait.assertTrue(server_2::isStarted);

      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT_2);
      Connection connection = factory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(session.createQueue("TEST"));
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      Queue queue_server_2 = locateQueue(server_2, "TEST");
      Queue queue_server_1 = locateQueue(server, "TEST");
      Queue queue_server_3 = locateQueue(server_3, "TEST");

      if (pagingSource) {
         queue_server_2.getPagingStore().startPaging();
      }

      if (pagingTarget) {
         queue_server_1.getPagingStore().startPaging();
         queue_server_3.getPagingStore().startPaging();
      }

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         Message message = session.createTextMessage(getText(largeMessage, i));
         message.setIntProperty("i", i);
         producer.send(message);
      }

      Wait.assertEquals(NUMBER_OF_MESSAGES, queue_server_2::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES, queue_server_3::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES, queue_server_1::getMessageCount);


      if (pagingTarget) {
         Assert.assertTrue(queue_server_1.getPagingStore().isPaging());
         Assert.assertTrue(queue_server_3.getPagingStore().isPaging());
      }

      if (pagingSource) {
         Assert.assertTrue(queue_server_2.getPagingStore().isPaging());
      }

      consumeMessages(largeMessage, 0, NUMBER_OF_MESSAGES / 2 - 1, AMQP_PORT_2, false);

      Wait.assertEquals(NUMBER_OF_MESSAGES / 2, queue_server_1::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES / 2, queue_server_2::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES / 2, queue_server_3::getMessageCount);

      // Replica is async, so we need to wait acks to arrive before we finish consuming there
      Wait.assertEquals(NUMBER_OF_MESSAGES / 2, queue_server_1::getMessageCount);


      consumeMessages(largeMessage, NUMBER_OF_MESSAGES / 2, NUMBER_OF_MESSAGES - 1, AMQP_PORT, true); // We consume on both servers as this is currently replicated
      consumeMessages(largeMessage, NUMBER_OF_MESSAGES / 2, NUMBER_OF_MESSAGES - 1, AMQP_PORT_3, true); // We consume on both servers as this is currently replicated
      consumeMessages(largeMessage, NUMBER_OF_MESSAGES / 2, NUMBER_OF_MESSAGES - 1, AMQP_PORT_2, true); // We consume on both servers as this is currently replicated


   }

   /** this might be helpful for debugging */
   private void printMessages(String printInfo, Queue queue) {
      System.out.println("*******************************************************************************************************************************");
      System.out.println(printInfo);
      System.out.println();
      LinkedListIterator<MessageReference> referencesIterator = queue.browserIterator();
      while (referencesIterator.hasNext()) {
         System.out.println("message " + referencesIterator.next().getMessage());
      }
      referencesIterator.close();
      System.out.println("*******************************************************************************************************************************");
   }

   private void consumeMessages(boolean largeMessage, int START_ID, int LAST_ID, int port, boolean assertNull) throws JMSException {
      ConnectionFactory cf = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + port);
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      conn.start();

      MessageConsumer consumer = sess.createConsumer(sess.createQueue("TEST"));
      for (int i = START_ID; i <= LAST_ID; i++) {
         Message message = consumer.receive(3000);
         Assert.assertNotNull(message);
         System.out.println("i::" + message.getIntProperty("i"));
         Assert.assertEquals(i, message.getIntProperty("i"));
         /*if (message instanceof TextMessage) {
            Assert.assertEquals(getText(largeMessage, i), ((TextMessage)message).getText());
         } */
      }
      if (assertNull) {
         Assert.assertNull(consumer.receiveNoWait());
      }
      conn.close();
   }

}
