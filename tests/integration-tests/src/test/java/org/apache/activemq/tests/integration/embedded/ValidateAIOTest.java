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
package org.apache.activemq.tests.integration.embedded;

import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.core.server.JournalType;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.junit.Test;

/**
 * Validate if the embedded server will start even with AIO selected
 *
 * @author Clebert Suconic
 */
public class ValidateAIOTest extends ServiceTestBase
{

   @Test
   public void testValidateAIO() throws Exception
   {
      Configuration config = createDefaultConfig(false)
         // This will force AsyncIO
         .setJournalType(JournalType.ASYNCIO);
      ActiveMQServer server = ActiveMQServers.newActiveMQServer(config, true);
      try
      {
         server.start();
      }
      finally
      {
         server.stop();
      }

   }
}
