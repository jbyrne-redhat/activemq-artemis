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
package org.apache.activemq.artemis.core.config.amqpbridging;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.uri.ConnectorTransportConfigurationParser;

public class AMQPConnectConfiguration implements Serializable {
   private static final long serialVersionUID = 8026604526022462048L;

   String name;
   String uri;
   String user = "x";
   String password = "x";

   List<TransportConfiguration> transportConfigurations;
   int reconnectAttempts;
   int retryInterval = 5000;

   List<AMQPConnectionElement> connectionElements;

   public AMQPConnectConfiguration(String name, String uri) {
      this.name = name;
      this.uri = uri;
   }

   public AMQPConnectConfiguration addElement(AMQPConnectionElement amqpConnectionElement) {
      if (connectionElements == null) {
         connectionElements = new ArrayList<>();
      }
      if (!(amqpConnectionElement instanceof AMQPReplica) && (amqpConnectionElement.getType() == AMQPConnectionAddressType.replica ||
          amqpConnectionElement.getType() == AMQPConnectionAddressType.copy)) {
         amqpConnectionElement = new AMQPReplica().setType(amqpConnectionElement.getType()).setMatchAddress(amqpConnectionElement.matchAddress);
      }
      amqpConnectionElement.setParent(this);

      connectionElements.add(amqpConnectionElement);

      return this;
   }

   public List<AMQPConnectionElement> getConnectionElements() {
      return connectionElements;
   }

   public void parseURI() throws Exception {
      ConnectorTransportConfigurationParser parser = new ConnectorTransportConfigurationParser(false);
      this.transportConfigurations = parser.newObject(uri, name);
   }

   public List<TransportConfiguration> getTransportConfigurations() throws Exception {
      if (transportConfigurations == null) {
         parseURI();
      }
      return transportConfigurations;
   }

   public int getReconnectAttempts() {
      return reconnectAttempts;
   }

   public AMQPConnectConfiguration setReconnectAttempts(int reconnectAttempts) {
      this.reconnectAttempts = reconnectAttempts;
      return this;
   }

   public String getUser() {
      return user;
   }

   public AMQPConnectConfiguration setUser(String user) {
      this.user = user;
      return this;
   }

   public String getPassword() {
      return password;
   }

   public AMQPConnectConfiguration setPassword(String password) {
      this.password = password;
      return this;
   }

   public int getRetryInterval() {
      return retryInterval;
   }

   public AMQPConnectConfiguration setRetryInterval(int retryInterval) {
      this.retryInterval = retryInterval;
      return this;
   }

   public String getUri() {
      return uri;
   }

   public AMQPConnectConfiguration setUri(String uri) {
      this.uri = uri;
      return this;
   }

   public String getName() {
      return name;
   }

   public AMQPConnectConfiguration setName(String name) {
      this.name = name;
      return this;
   }
}
