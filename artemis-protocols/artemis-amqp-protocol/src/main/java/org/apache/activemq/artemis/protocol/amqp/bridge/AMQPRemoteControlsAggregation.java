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
package org.apache.activemq.artemis.protocol.amqp.bridge;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.remotecontrol.RemoteControl;

public class AMQPRemoteControlsAggregation implements RemoteControl, ActiveMQComponent {


   List<AMQPRemoteControlsSource> partitions = new ArrayList<>();

   public void addPartition(AMQPRemoteControlsSource partition) {
      this.partitions.add(partition);
   }

   public void removeParition(AMQPRemoteControlsSource partition) {
      this.partitions.remove(partition);
   }


   @Override
   public void start() throws Exception {

   }

   @Override
   public void stop() throws Exception {

   }

   @Override
   public boolean isStarted() {
      return false;
   }

   @Override
   public void addAddress(AddressInfo addressInfo) throws Exception {
      for (RemoteControl partition : partitions) {
         partition.addAddress(addressInfo);
      }

   }

   @Override
   public void deleteAddress(AddressInfo addressInfo) throws Exception {
      for (RemoteControl partition : partitions) {
         partition.deleteAddress(addressInfo);
      }
   }

   @Override
   public void createQueue(QueueConfiguration queueConfiguration) throws Exception {
      for (RemoteControl partition : partitions) {
         partition.createQueue(queueConfiguration);
      }
   }

   @Override
   public void deleteQueue(SimpleString addressName, SimpleString queueName) throws Exception {
      for (RemoteControl partition : partitions) {
         partition.deleteQueue(addressName, queueName);
      }
   }

   @Override
   public void sendMessage(Message message, RoutingContext context, List<MessageReference> refs) {
      for (RemoteControl partition : partitions) {
         partition.sendMessage(message, context, refs);
      }
   }

   @Override
   public void routingDone(List<MessageReference> refs, boolean direct) {
      for (RemoteControl partition : partitions) {
         partition.routingDone(refs, direct);
      }
   }

   @Override
   public void postAcknowledge(MessageReference ref, AckReason reason) throws Exception {
      for (RemoteControl partition : partitions) {
         partition.postAcknowledge(ref, reason);
      }
   }
}
