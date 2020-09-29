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

public class AMQPReplica implements Serializable {

   final SimpleString snfQueue;
   final boolean acks;

   public AMQPReplica(SimpleString snfQueue, boolean acks) {
      this.snfQueue = snfQueue;
      this.acks = acks;
   }

   public AMQPReplica(String snfQueue, boolean acks) {
      this(SimpleString.toSimpleString(snfQueue),  acks);
   }

   public boolean isAcks() {
      return acks;
   }

   public SimpleString getSnfQueue() {
      return snfQueue;
   }

   @Override
   public String toString() {
      return "AMQPReplica{" + "snfQueue=" + snfQueue + ", acks=" + acks + '}';
   }
}
