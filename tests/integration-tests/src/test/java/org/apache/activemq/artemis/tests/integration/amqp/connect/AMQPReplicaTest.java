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
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.amqpbridging.AMQPConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpbridging.AMQPReplica;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
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
   public void testReplicaDisconnect() throws Exception {
   }

   @Test
   public void testReplicaCatchupOnQueueCreates() throws Exception {
      server.setIdentity("Server1");
      server.stop();

      server_2 = createServer(AMQP_PORT_2, false);

      AMQPConnectConfiguration amqpConnection = new AMQPConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
      amqpConnection.setReplica(new AMQPReplica("SNFREPLICA", true));
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      server_2.start();

      server_2.addAddressInfo(new AddressInfo("sometest").setAutoCreated(false));
      server_2.createQueue(new QueueConfiguration("sometest").setDurable(true));

      Wait.assertTrue(() -> server_2.locateQueue("sometest") != null);

      server_2.stop();

      server.start();
      Assert.assertTrue(server.locateQueue("sometest") == null);
      Wait.assertTrue(server::isActive);
      server_2.start();
      // if this does not succeed the catch up did not arrive at the other server
      Wait.assertTrue(() -> server.locateQueue("sometest") != null);
      server_2.stop();
      server.stop();
   }

   @Test
   public void testReplicaCatchupOnQueueCreatesAndDeletes() throws Exception {
      server.setIdentity("Server1");
      server.addAddressInfo(new AddressInfo("sometest").setAutoCreated(false).addRoutingType(RoutingType.MULTICAST));
      // This queue will disappear from the source, so it should go
      server.createQueue(new QueueConfiguration("ToBeGone").setDurable(true).setRoutingType(RoutingType.MULTICAST));
      server.stop();

      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("server_2");

      AMQPConnectConfiguration amqpConnection = new AMQPConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
      amqpConnection.setReplica(new AMQPReplica("SNFREPLICA", true));
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      server_2.start();

      server_2.addAddressInfo(new AddressInfo("sometest").setAutoCreated(false).addRoutingType(RoutingType.MULTICAST));
      server_2.createQueue(new QueueConfiguration("sometest").setDurable(true).setRoutingType(RoutingType.MULTICAST));

      Wait.assertTrue(() -> server_2.locateQueue("sometest") != null);

      server_2.stop();

      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("server_2");

      amqpConnection = new AMQPConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
      amqpConnection.setReplica(new AMQPReplica("SNFREPLICA", true));
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      server.start();
      Assert.assertTrue(server.locateQueue("sometest") == null);
      Assert.assertTrue(server.locateQueue("ToBeGone") != null);
      Wait.assertTrue(server::isActive);
      server_2.start();
      // if this does not succeed the catch up did not arrive at the other server
      Wait.assertTrue(() -> server.locateQueue("sometest") != null);
      Wait.assertTrue(() -> server.locateQueue("ToBeGone") == null);
      server_2.stop();
      server.stop();
   }

   @Test
   public void testReplicaLargeMessages() throws Exception {
      replicaTest(true, true, false, false);
   }

   @Test
   public void testReplicaLargeMessagesPagingEverywhere() throws Exception {
      replicaTest(true, true, true, true);
   }


   @Test
   public void testReplica() throws Exception {
      replicaTest(false, true, false, false);
   }

   @Test
   public void testReplicaNoAcks() throws Exception {
      replicaTest(false, false, false, false);
   }

   @Test
   public void testReplicaPagedTarget() throws Exception {
      replicaTest(false, true, true, false);
   }

   @Test
   public void testReplicaPagingEverywhere() throws Exception {
      replicaTest(false, true, true, true);
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

   private void replicaTest(boolean largeMessage, boolean acks, boolean pagingTarget, boolean pagingSource) throws Exception {
      server.setIdentity("targetServer");
      server_2 = createServer(AMQP_PORT_2, false);

      AMQPConnectConfiguration amqpConnection = new AMQPConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
      amqpConnection.setReplica(new AMQPReplica("SNFREPLICA", acks));
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      int NUMBER_OF_MESSAGES = 100;

      server_2.start();
      Wait.assertTrue(server_2::isStarted);

      // We create the address to avoid auto delete on the queue
      server_2.addAddressInfo(new AddressInfo("TEST").addRoutingType(RoutingType.ANYCAST).setAutoCreated(false));
      server_2.createQueue(new QueueConfiguration("TEST").setRoutingType(RoutingType.ANYCAST).setAddress("TEST").setAutoCreated(false));

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


      Queue snfreplica = server_2.locateQueue("SNFREPLICA");

      Assert.assertNotNull(snfreplica);

      Wait.assertEquals(0, snfreplica::getMessageCount);


      Wait.assertEquals(NUMBER_OF_MESSAGES, queueOnServer1::getMessageCount);
      Queue queueOnServer2 = locateQueue(server_2, "TEST");
      Wait.assertEquals(NUMBER_OF_MESSAGES, queueOnServer1::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES, queueOnServer2::getMessageCount);

      if (pagingTarget) {
         assertTrue(queueOnServer1.getPagingStore().isPaging());
      }

      if (acks) {
         consumeMessages(largeMessage, 0, NUMBER_OF_MESSAGES / 2 - 1, AMQP_PORT_2, false);
         // Replica is async, so we need to wait acks to arrive before we finish consuming there
         Wait.assertEquals(NUMBER_OF_MESSAGES / 2, queueOnServer1::getMessageCount);
         // we consume on replica, as half the messages were acked
         consumeMessages(largeMessage, NUMBER_OF_MESSAGES / 2, NUMBER_OF_MESSAGES - 1, AMQP_PORT, true); // We consume on both servers as this is currently replicated

         if (largeMessage) {
            validateNoFilesOnLargeDir(server.getConfiguration().getLargeMessagesDirectory(), 0);
            validateNoFilesOnLargeDir(server_2.getConfiguration().getLargeMessagesDirectory(), 50); // we kept half of the messages
         }
      } else {

         consumeMessages(largeMessage, 0, NUMBER_OF_MESSAGES - 1, AMQP_PORT_2, true);
         consumeMessages(largeMessage, 0, NUMBER_OF_MESSAGES - 1, AMQP_PORT, true);
         if (largeMessage) {
            validateNoFilesOnLargeDir(server.getConfiguration().getLargeMessagesDirectory(), 0);
            validateNoFilesOnLargeDir(server_2.getConfiguration().getLargeMessagesDirectory(), 0); // we kept half of the messages
         }
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

      Queue replica1Queue = server_2.locateQueue("REPLICA1");
      Queue replica2Queue = server_2.locateQueue("REPLICA2");

      Wait.assertEquals(0L, replica2Queue.getPagingStore()::getAddressSize, 1000, 100);
      Wait.assertEquals(0L, replica1Queue.getPagingStore()::getAddressSize, 1000, 100);


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

      validateNoFilesOnLargeDir(server.getConfiguration().getLargeMessagesDirectory(), 0);
      validateNoFilesOnLargeDir(server_3.getConfiguration().getLargeMessagesDirectory(), 0);
      validateNoFilesOnLargeDir(server_2.getConfiguration().getLargeMessagesDirectory(), 0);

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
         System.out.println("port " + port + ",i::" + message.getIntProperty("i"));
         Assert.assertEquals(i, message.getIntProperty("i"));
         if (message instanceof TextMessage) {
            Assert.assertEquals(getText(largeMessage, i), ((TextMessage)message).getText());
         }
      }
      if (assertNull) {
         Assert.assertNull(consumer.receiveNoWait());
      }
      conn.close();
   }

}
