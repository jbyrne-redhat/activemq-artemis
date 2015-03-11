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

import org.apache.activemq.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.i18n.I18NFactory;
import org.apache.activemq.i18n.annotation.Bundle;
import org.apache.activemq.i18n.annotation.Message;

/**
 *         Logger Code 23
 *         <p/>
 *         each message id must be 6 digits long starting with 10, the 3rd digit should be 9
 *         <p/>
 *         so 239000 to 239999
 */
@Bundle(projectCode = "AMQ")
public interface ActiveMQAeroGearBundle
{
   ActiveMQAeroGearBundle BUNDLE = I18NFactory.getMessageBundle(ActiveMQAeroGearBundle.class);

   @Message(id = 239000, value = "endpoint can not be null")
   ActiveMQIllegalStateException endpointNull();

   @Message(id = 239001, value = "application-id can not be null")
   ActiveMQIllegalStateException applicationIdNull();

   @Message(id = 239002, value = "master-secret can not be null")
   ActiveMQIllegalStateException masterSecretNull();

   @Message(id = 239003, value = "{0}: queue {1} not found")
   ActiveMQIllegalStateException noQueue(String connectorName, String queueName);
   
   

   // Logger messages

   @Message(id = 231001, value = "aerogear connector connected to {0}", format = Message.Format.MESSAGE_FORMAT)
   void connected(String endpoint);

   @Message(id = 232003, value = "removing aerogear connector as credentials are invalid", format = Message.Format.MESSAGE_FORMAT)
   void reply401();

   @Message(id = 232004, value = "removing aerogear connector as endpoint is invalid", format = Message.Format.MESSAGE_FORMAT)
   void reply404();

   @Message(id = 232005, value = "removing aerogear connector as unexpected respone {0} returned", format = Message.Format.MESSAGE_FORMAT)
   void replyUnknown(int status);

   @Message(id = 232006, value = "unable to connect to aerogear server, retrying in {0} seconds", format = Message.Format.MESSAGE_FORMAT)
   void sendFailed(int retry);

   @Message(id = 232007, value = "removing aerogear connector unable to connect after {0} attempts, giving up", format = Message.Format.MESSAGE_FORMAT)
   void unableToReconnect(int retryAttempt);

}
