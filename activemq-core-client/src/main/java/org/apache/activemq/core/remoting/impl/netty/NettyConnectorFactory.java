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
package org.apache.activemq.core.remoting.impl.netty;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.spi.core.remoting.BufferHandler;
import org.apache.activemq.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.spi.core.remoting.ConnectionLifeCycleListener;
import org.apache.activemq.spi.core.remoting.Connector;
import org.apache.activemq.spi.core.remoting.ConnectorFactory;

/**
 * A NettyConnectorFactory
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */
public class NettyConnectorFactory implements ConnectorFactory
{
   public Connector createConnector(final Map<String, Object> configuration,
                                    final BufferHandler handler,
                                    final ConnectionLifeCycleListener listener,
                                    final Executor closeExecutor,
                                    final Executor threadPool,
                                    final ScheduledExecutorService scheduledThreadPool,
                                    final ClientProtocolManager protocolManager)
   {
      return new NettyConnector(configuration, handler, listener, closeExecutor, threadPool, scheduledThreadPool);
   }

   public Set<String> getAllowableProperties()
   {
      return TransportConstants.ALLOWABLE_CONNECTOR_KEYS;
   }

   @Override
   public boolean isReliable()
   {
      return false;
   }

   @Override
   public Map<String, Object> getDefaults()
   {
      return NettyConnector.DEFAULT_CONFIG;
   }
}
