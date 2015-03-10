/**
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
package org.apache.activemq.tests.integration.cluster.bridge;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.core.remoting.impl.invm.InVMConnector;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.core.server.NodeManager;
import org.apache.activemq.tests.util.InVMNodeManagerServer;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.After;

public abstract class BridgeTestBase extends UnitTestCase
{

   @Override
   @After
   public void tearDown() throws Exception
   {
      InVMConnector.failOnCreateConnection = false;

      super.tearDown();
   }

   protected ActiveMQServer createActiveMQServer(final int id, final boolean netty, final Map<String, Object> params) throws Exception
   {
      return createActiveMQServer(id, params, netty, null);
   }

   protected ActiveMQServer createActiveMQServer(final int id, final boolean netty, final Map<String, Object> params, NodeManager nodeManager) throws Exception
   {
      return createActiveMQServer(id, params, netty, nodeManager);
   }

   protected ActiveMQServer createActiveMQServer(final int id,
                                                 final Map<String, Object> params,
                                                 final boolean netty,
                                                 final NodeManager nodeManager) throws Exception
   {
      TransportConfiguration tc = new TransportConfiguration();

      if (netty)
      {
         params.put(org.apache.activemq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                    org.apache.activemq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + id);
         tc = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);

      }
      else
      {
         params.put(org.apache.activemq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, id);
         tc = new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params);
      }
      Configuration serviceConf = createBasicConfig()
         .setJournalType(getDefaultJournalType())
         .setBindingsDirectory(getBindingsDir(id, false))
         .setJournalMinFiles(2)
         .setJournalDirectory(getJournalDir(id, false))
         .setPagingDirectory(getPageDir(id, false))
         .setLargeMessagesDirectory(getLargeMessagesDir(id, false))
         // these tests don't need any big storage so limiting the size of the journal files to speed up the test
         .setJournalFileSize(100 * 1024)
         .addAcceptorConfiguration(tc)
         .setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration());

      ActiveMQServer service;
      if (nodeManager == null)
      {
         service = ActiveMQServers.newActiveMQServer(serviceConf, true);
      }
      else
      {
         service = new InVMNodeManagerServer(serviceConf, nodeManager);
      }

      return addServer(service);
   }

   protected ActiveMQServer createBackupActiveMQServer(final int id,
                                                       final Map<String, Object> params,
                                                       final boolean netty,
                                                       final int liveId,
                                                       final NodeManager nodeManager) throws Exception
   {
      TransportConfiguration tc = new TransportConfiguration();

      if (netty)
      {
         params.put(org.apache.activemq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                    org.apache.activemq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + id);
         tc = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);

      }
      else
      {
         params.put(org.apache.activemq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, id);
         tc = new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params);
      }

      Configuration serviceConf = createBasicConfig()
         .setJournalType(getDefaultJournalType())
         .setBindingsDirectory(getBindingsDir(liveId, false))
         .setJournalMinFiles(2)
         .setJournalDirectory(getJournalDir(liveId, false))
         .setPagingDirectory(getPageDir(liveId, false))
         .setLargeMessagesDirectory(getLargeMessagesDir(liveId, false))
         // these tests don't need any big storage so limiting the size of the journal files to speed up the test
         .setJournalFileSize(100 * 1024)
         .addAcceptorConfiguration(tc)
         .setHAPolicyConfiguration(new SharedStoreSlavePolicyConfiguration());

      ActiveMQServer service;
      if (nodeManager == null)
      {
         service = ActiveMQServers.newActiveMQServer(serviceConf, true);
      }
      else
      {
         service = new InVMNodeManagerServer(serviceConf, nodeManager);
      }
      return addServer(service);
   }


   protected void waitForServerStart(ActiveMQServer server) throws Exception
   {
      if (!server.waitForActivation(5000L, TimeUnit.MILLISECONDS))
         throw new IllegalStateException("Timed out waiting for server starting = " + server);
   }
}
