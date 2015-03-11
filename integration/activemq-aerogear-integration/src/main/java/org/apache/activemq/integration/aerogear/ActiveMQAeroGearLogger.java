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
package org.apache.activemq.integration.aerogear;

import org.apache.activemq.i18n.I18NFactory;
import org.apache.activemq.i18n.annotation.Bundle;
import org.apache.activemq.i18n.annotation.LogMessage;
import org.apache.activemq.i18n.annotation.Message;

/**
 * Logger Code 23
 *
 * each message id must be 6 digits long starting with 18, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 181000 to 181999
 */
@Bundle(projectCode = "AMQ")
public interface ActiveMQAeroGearLogger
{
   /**
    * The aerogear logger.
    */
   ActiveMQAeroGearLogger LOGGER = I18NFactory.getMessageLogger(ActiveMQAeroGearLogger.class);

   void connected(String endpoint);

   @LogMessage(level = LogMessage.Level.WARN, id = 232003, value = "removing aerogear connector as credentials are invalid", format = Message.Format.MESSAGE_FORMAT)
   void reply401();

   @LogMessage(level = LogMessage.Level.WARN, id = 232004, value = "removing aerogear connector as endpoint is invalid", format = Message.Format.MESSAGE_FORMAT)
   void reply404();

   @LogMessage(level = LogMessage.Level.WARN, id = 232005, value = "removing aerogear connector as unexpected respone {0} returned", format = Message.Format.MESSAGE_FORMAT)
   void replyUnknown(int status);

   @LogMessage(level = LogMessage.Level.WARN, id = 232006, value = "unable to connect to aerogear server, retrying in {0} seconds", format = Message.Format.MESSAGE_FORMAT)
   void sendFailed(int retry);

   @LogMessage(level = LogMessage.Level.WARN, id = 232007, value = "removing aerogear connector unable to connect after {0} attempts, giving up", format = Message.Format.MESSAGE_FORMAT)
   void unableToReconnect(int retryAttempt);
}
