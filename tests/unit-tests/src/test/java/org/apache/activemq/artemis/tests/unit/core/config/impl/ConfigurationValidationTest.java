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
package org.apache.activemq.artemis.tests.unit.core.config.impl;

import org.apache.activemq.artemis.core.config.amqpbridging.AMQPConnectConfiguration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.amqpbridging.AMQPConnectionAddressType;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.XMLUtil;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Element;

public class ConfigurationValidationTest extends ActiveMQTestBase {

   static {
      System.setProperty("a2Prop", "a2");
      System.setProperty("falseProp", "false");
      System.setProperty("trueProp", "true");
      System.setProperty("ninetyTwoProp", "92");
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * test does not pass in eclipse (because it can not find artemis-configuration.xsd).
    * It runs fine on the CLI with the proper env setting.
    */
   @Test
   public void testMinimalConfiguration() throws Exception {
      String xml = "<core xmlns='urn:activemq:core'>" + "</core>";
      Element element = XMLUtil.stringToElement(xml);
      Assert.assertNotNull(element);
      XMLUtil.validate(element, "schema/artemis-configuration.xsd");
   }

   @Test
   public void testFullConfiguration() throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager("ConfigurationTest-full-config.xml");
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();

      Assert.assertEquals(true, fc.isPersistDeliveryCountBeforeDelivery());
   }

   @Test
   public void testAMQPConnectParsing() throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager("ConfigurationTest-full-config.xml");
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();

      Assert.assertEquals(2, fc.getAMQPConnection().size());

      AMQPConnectConfiguration amqpConnectConfiguration = fc.getAMQPConnection().get(0);
      Assert.assertEquals(33, amqpConnectConfiguration.getReconnectAttempts());
      Assert.assertEquals(333, amqpConnectConfiguration.getRetryInterval());
      Assert.assertEquals("test1", amqpConnectConfiguration.getName());
      Assert.assertEquals("tcp://test1:111", amqpConnectConfiguration.getUri());

      Assert.assertEquals("TEST-PUSH", amqpConnectConfiguration.getConnectionAddresses().get(0).getMatchAddress());
      Assert.assertEquals(AMQPConnectionAddressType.push, amqpConnectConfiguration.getConnectionAddresses().get(0).getType());
      Assert.assertEquals("TEST-PULL", amqpConnectConfiguration.getConnectionAddresses().get(1).getMatchAddress());
      Assert.assertEquals(AMQPConnectionAddressType.pull, amqpConnectConfiguration.getConnectionAddresses().get(1).getType());
      Assert.assertEquals("TEST-DUAL", amqpConnectConfiguration.getConnectionAddresses().get(2).getMatchAddress());
      Assert.assertEquals(AMQPConnectionAddressType.dual, amqpConnectConfiguration.getConnectionAddresses().get(2).getType());

      Assert.assertEquals("TestWithoutAcks", amqpConnectConfiguration.getReplica().getSnfQueue().toString());
      Assert.assertFalse(amqpConnectConfiguration.getReplica().isAcks());

      amqpConnectConfiguration = fc.getAMQPConnection().get(1);
      Assert.assertEquals("test2", amqpConnectConfiguration.getName());
      Assert.assertEquals("tcp://test2:222", amqpConnectConfiguration.getUri());
      Assert.assertTrue(amqpConnectConfiguration.getReplica().isAcks());
      Assert.assertEquals("TestWithAcks", amqpConnectConfiguration.getReplica().getSnfQueue().toString());

   }

   @Test
   public void testChangeConfiguration() throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager("ConfigurationTest-full-config.xml");
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();
      deploymentManager = new FileDeploymentManager("ConfigurationTest-full-config-wrong-address.xml");
      deploymentManager.addDeployable(fc);

      try {
         deploymentManager.readConfiguration();
         fail("Exception expected");
      } catch (Exception ignored) {
      }
   }
}
