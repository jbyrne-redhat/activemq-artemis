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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.net.URL;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.ExecuteUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class QpidRouterPeerTest extends AmqpClientTestSupport {

   ExecuteUtil.ProcessHolder qpidProcess;

   /**
    * This will validate if the environemnt has qdrouterd installed and if this test can be used or not.
    */
   @BeforeClass
   public static void validateqdrotuer() {
      try {
         int result = ExecuteUtil.runCommand(true, "qdrouterd", "--version");
         Assume.assumeTrue("qdrouterd does not exist", result == 0);
      } catch (Exception e) {
         e.printStackTrace();
         Assume.assumeNoException(e);
      }
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      return createServer(AMQP_PORT, false);
   }

   @Before
   public void startQpidRouter() throws Exception {
      URL qpidConfig = this.getClass().getClassLoader().getResource("QpidRouterPeerTest-qpidr.conf");
      qpidProcess = ExecuteUtil.run(true, "qdrouterd", "-c", qpidConfig.getFile());
   }

   @After
   public void stopQpidRouter() throws Exception {
      qpidProcess.kill();
   }

   @Test
   public void testQpidRouter() throws Exception {
      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:24621").setRetryInterval(10).setReconnectAttempts(-1);
      amqpConnection.addElement(new AMQPBrokerConnectionElement().setMatchAddress("queue.test").setType(AMQPBrokerConnectionAddressType.peer));
      server.getConfiguration().addAMQPConnection(amqpConnection);
      server.start();
      server.addAddressInfo(new AddressInfo("queue.test").addRoutingType(RoutingType.ANYCAST).setAutoCreated(true).setTemporary(false));
      server.createQueue(new QueueConfiguration("queue.test").setAddress("queue.test").setRoutingType(RoutingType.ANYCAST));

      ConnectionFactory factoryProducer = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:24622");
      Connection connection = null;

      for (int i = 0; i < 100; i++) {
         try {
            // Some retry
            connection = factoryProducer.createConnection();
            break;
         } catch (Exception e) {
            Thread.sleep(10);
         }
      }
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue("queue.test");
      MessageProducer producer = session.createProducer(queue);

      org.apache.activemq.artemis.core.server.Queue testQueueOnServer = server.locateQueue("queue.test");

      for (int i = 0; i < 100; i++) {
         producer.send(session.createTextMessage("hello " + i));
      }

      Wait.assertEquals(100, testQueueOnServer::getMessageCount);

      connection.close();

      System.out.println("*******************************************************************************************************************************");
      System.out.println("Creating consumer");

      ConnectionFactory factoryConsumer = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:24622");
      Connection connectionConsumer = factoryConsumer.createConnection();
      Session sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queueConsumer = sessionConsumer.createQueue("queue.test");
      MessageConsumer consumer = sessionConsumer.createConsumer(queueConsumer);
      connectionConsumer.start();

      try {
         for (int i = 0; i < 100; i++) {
            TextMessage received = (TextMessage) consumer.receive(5000);
            if (received == null) {
               System.out.println("*******************************************************************************************************************************");
               System.out.println("qdstat after message timed out:");
               ExecuteUtil.runCommand(true, "qdstat", "-b", "127.0.0.1:24622", "-l");
               System.out.println("*******************************************************************************************************************************");
            }
            Assert.assertNotNull(received);
            Assert.assertEquals("hello " + i, received.getText());
         }
      } finally {
         try {
            connectionConsumer.close();
         } catch (Throwable ignored) {

         }
      }

   }
}
