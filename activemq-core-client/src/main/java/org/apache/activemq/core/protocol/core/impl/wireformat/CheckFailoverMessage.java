/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.core.protocol.core.impl.wireformat;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;

public class CheckFailoverMessage extends PacketImpl
{
   private String nodeID;

   public CheckFailoverMessage(final String nodeID)
   {
      super(CHECK_FOR_FAILOVER);
      this.nodeID = nodeID;
   }

   public CheckFailoverMessage()
   {
      super(CHECK_FOR_FAILOVER);
   }

   @Override
   public void encodeRest(ActiveMQBuffer buffer)
   {
      buffer.writeNullableString(nodeID);
   }

   @Override
   public void decodeRest(ActiveMQBuffer buffer)
   {
      nodeID = buffer.readNullableString();
   }

   public String getNodeID()
   {
      return nodeID;
   }
}
