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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.postoffice.impl.AddressImpl;

public class AMQPConnectionElement implements Serializable {
   SimpleString matchAddress;
   SimpleString queueName;
   AMQPConnectionAddressType type;
   AMQPConnectConfiguration parent;

   public AMQPConnectionElement() {
   }

   public AMQPConnectConfiguration getParent() {
      return parent;
   }

   public AMQPConnectionElement setParent(AMQPConnectConfiguration parent) {
      this.parent = parent;
      return this;
   }

   public SimpleString getQueueName() {
      return queueName;
   }

   public AMQPConnectionElement setQueueName(SimpleString queueName) {
      this.queueName = queueName;
      return this;
   }

   public SimpleString getMatchAddress() {
      return matchAddress;
   }

   public boolean match(SimpleString checkAddress, WildcardConfiguration wildcardConfig) {
      AddressImpl thisAddress = new AddressImpl(matchAddress, wildcardConfig);
      AddressImpl otherAddress = new AddressImpl(checkAddress, wildcardConfig);
      return thisAddress.matches(otherAddress);
   }

   public AMQPConnectionElement setMatchAddress(String matchAddress) {
      return this.setMatchAddress(SimpleString.toSimpleString(matchAddress));
   }

   public AMQPConnectionElement setMatchAddress(SimpleString matchAddress) {
      this.matchAddress = matchAddress;
      return this;
   }

   public AMQPConnectionAddressType getType() {
      return type;
   }

   public AMQPConnectionElement setType(AMQPConnectionAddressType type) {
      this.type = type;
      return this;
   }
}
