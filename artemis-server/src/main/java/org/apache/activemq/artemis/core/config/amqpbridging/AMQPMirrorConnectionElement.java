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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.utils.RandomUtil;

public class AMQPMirrorConnectionElement extends AMQPConnectionElement {

   SimpleString sourceMirrorAddress;

   SimpleString targetMirrorAddress = SimpleString.toSimpleString("$mirror");

   boolean durable;

   boolean queueCreation = true;

   boolean queueRemoval = true;

   boolean messageAcknowledgements = true;

   public AMQPMirrorConnectionElement() {
      this.setType(AMQPConnectionAddressType.mirror);
   }

   /** There is no setter for this property.
    * Basically by setting a sourceMirrorAddress we are automatically setting this to true. */
   public boolean isDurable() {
      return durable;
   }

   public AMQPMirrorConnectionElement setSourceMirrorAddress(String mirrorAddress) {
      return this.setSourceMirrorAddress(SimpleString.toSimpleString(mirrorAddress));
   }
   public AMQPMirrorConnectionElement setSourceMirrorAddress(SimpleString souceMirrorAddress) {
      this.sourceMirrorAddress = souceMirrorAddress;
      this.durable = sourceMirrorAddress != null;
      return this;
   }

   public SimpleString getSourceMirrorAddress() {
      if (sourceMirrorAddress == null) {
         sourceMirrorAddress = SimpleString.toSimpleString(parent.getName() + RandomUtil.randomString());
      }
      return sourceMirrorAddress;
   }

   public SimpleString getTargetMirrorAddress() {
      return targetMirrorAddress;
   }

   public AMQPMirrorConnectionElement setTargetMirrorAddress(String targetMirrorAddress) {
      return this.setTargetMirrorAddress(SimpleString.toSimpleString(targetMirrorAddress));
   }

   public AMQPMirrorConnectionElement setTargetMirrorAddress(SimpleString targetMirrorAddress) {
      this.targetMirrorAddress = targetMirrorAddress;
      return this;
   }

   public boolean isQueueCreation() {
      return queueCreation;
   }

   public AMQPMirrorConnectionElement setQueueCreation(boolean queueCreation) {
      this.queueCreation = queueCreation;
      return this;
   }

   public boolean isQueueRemoval() {
      return queueRemoval;
   }

   public AMQPMirrorConnectionElement setQueueRemoval(boolean queueRemoval) {
      this.queueRemoval = queueRemoval;
      return this;
   }

   @Override
   public AMQPMirrorConnectionElement setType(AMQPConnectionAddressType type) {
      super.setType(type);
      return this;
   }

   public boolean isMessageAcknowledgements() {
      return messageAcknowledgements;
   }

   public AMQPMirrorConnectionElement setMessageAcknowledgements(boolean messageAcknowledgements) {
      this.messageAcknowledgements = messageAcknowledgements;
      return this;
   }
}
