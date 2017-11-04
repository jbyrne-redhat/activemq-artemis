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
package org.apache.activemq.artemis.jms.client;

import javax.jms.Queue;

/**
 * ActiveMQ Artemis implementation of a JMS Queue.
 * <br>
 * This class can be instantiated directly.
 */
public class ActiveMQQueue extends ActiveMQDestination implements Queue {

   // Constants -----------------------------------------------------
   private static final long serialVersionUID = -1106092883162295462L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------
   public ActiveMQQueue() {
      this(null);
   }

   public ActiveMQQueue(final String address) {
      super(address, TYPE.QUEUE, null);
   }

   public ActiveMQQueue(final String address, boolean temporary) {
      super(address, temporary ? TYPE.TEMP_QUEUE : TYPE.QUEUE, null);
   }

   /**
    * @param address
    * @param temporary
    * @param session
    */
   public ActiveMQQueue(String address, boolean temporary, ActiveMQSession session) {
      super(address, temporary ? TYPE.TEMP_QUEUE : TYPE.QUEUE, session);
   }


   // Queue implementation ------------------------------------------

   // Public --------------------------------------------------------

   @Override
   public String getQueueName() {
      return getAddress();
   }

   @Override
   public String toString() {
      return "ActiveMQQueue[" + getAddress() + "]";
   }

   @Override
   public boolean equals(final Object o) {
      if (this == o) {
         return true;
      }

      if (!(o instanceof ActiveMQQueue)) {
         return false;
      }

      ActiveMQQueue that = (ActiveMQQueue) o;

      return super.getAddress().equals(that.getAddress());
   }

   @Override
   public int hashCode() {
      return super.getAddress().hashCode();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
