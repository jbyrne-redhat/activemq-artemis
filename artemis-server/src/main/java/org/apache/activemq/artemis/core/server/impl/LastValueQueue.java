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
package org.apache.activemq.artemis.core.server.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;

/**
 * A queue that will discard messages if a newer message with the same
 * {@link org.apache.activemq.artemis.core.message.impl.MessageImpl#HDR_LAST_VALUE_NAME} property value. In other words it only retains the last
 * value
 * <p>
 * This is useful for example, for stock prices, where you're only interested in the latest value
 * for a particular stock
 */
public class LastValueQueue extends QueueImpl
{
   private final Map<SimpleString, HolderReference> map = new ConcurrentHashMap<SimpleString, HolderReference>();

   public LastValueQueue(final long persistenceID,
                         final SimpleString address,
                         final SimpleString name,
                         final Filter filter,
                         final PageSubscription pageSubscription,
                         final SimpleString user,
                         final boolean durable,
                         final boolean temporary,
                         final boolean autoCreated,
                         final ScheduledExecutorService scheduledExecutor,
                         final PostOffice postOffice,
                         final StorageManager storageManager,
                         final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                         final Executor executor)
   {
      super(persistenceID,
            address,
            name,
            filter,
            pageSubscription,
            user,
            durable,
            temporary,
            autoCreated,
            scheduledExecutor,
            postOffice,
            storageManager,
            addressSettingsRepository,
            executor);
      new Exception("LastValueQeue " + this ).toString();
   }

   @Override
   public synchronized void addTail(final MessageReference ref, final boolean direct)
   {
      SimpleString prop = ref.getMessage().getSimpleStringProperty(Message.HDR_LAST_VALUE_NAME);

      if (prop != null)
      {
         HolderReference hr = map.get(prop);

         if (hr != null)
         {
            // We need to overwrite the old ref with the new one and ack the old one

            MessageReference oldRef = hr.getReference();

            referenceHandled();

            try
            {
               oldRef.acknowledge();
            }
            catch (Exception e)
            {
               ActiveMQServerLogger.LOGGER.errorAckingOldReference(e);
            }

            hr.setReference(ref);

         }
         else
         {
            hr = new HolderReference(prop, ref);

            map.put(prop, hr);

            super.addTail(hr, direct);
         }
      }
      else
      {
         super.addTail(ref, direct);
      }
   }

   @Override
   public synchronized void addHead(final MessageReference ref)
   {
      SimpleString prop = ref.getMessage().getSimpleStringProperty(Message.HDR_LAST_VALUE_NAME);

      if (prop != null)
      {
         HolderReference hr = map.get(prop);

         if (hr != null)
         {
            // We keep the current ref and ack the one we are returning

            super.referenceHandled();

            try
            {
               super.acknowledge(ref);
            }
            catch (Exception e)
            {
               ActiveMQServerLogger.LOGGER.errorAckingOldReference(e);
            }
         }
         else
         {
            map.put(prop, (HolderReference)ref);

            super.addHead(ref);
         }
      }
      else
      {
         super.addHead(ref);
      }
   }


   @Override
   protected void refRemoved(MessageReference ref)
   {
      synchronized (this)
      {
         SimpleString prop = ref.getMessage().getSimpleStringProperty(Message.HDR_LAST_VALUE_NAME);

         if (prop != null)
         {
            map.remove(prop);
         }
      }

      super.refRemoved(ref);
   }

   private class HolderReference implements MessageReference
   {
      private final SimpleString prop;

      private volatile MessageReference ref;

      private Long consumerId;

      HolderReference(final SimpleString prop, final MessageReference ref)
      {
         this.prop = prop;

         this.ref = ref;
      }

      MessageReference getReference()
      {
         return ref;
      }

      public void handled()
      {
         ref.handled();
         // We need to remove the entry from the map just before it gets delivered
         map.remove(prop);
      }

      @Override
      public void setAlreadyAcked()
      {
         ref.setAlreadyAcked();
      }

      @Override
      public boolean isAlreadyAcked()
      {
         return ref.isAlreadyAcked();
      }

      void setReference(final MessageReference ref)
      {
         this.ref = ref;
      }

      public MessageReference copy(final Queue queue)
      {
         return ref.copy(queue);
      }

      public void decrementDeliveryCount()
      {
         ref.decrementDeliveryCount();
      }

      public int getDeliveryCount()
      {
         return ref.getDeliveryCount();
      }

      public ServerMessage getMessage()
      {
         return ref.getMessage();
      }

      public Queue getQueue()
      {
         return ref.getQueue();
      }

      public long getScheduledDeliveryTime()
      {
         return ref.getScheduledDeliveryTime();
      }

      public void incrementDeliveryCount()
      {
         ref.incrementDeliveryCount();
      }

      public void setDeliveryCount(final int deliveryCount)
      {
         ref.setDeliveryCount(deliveryCount);
      }

      public void setScheduledDeliveryTime(final long scheduledDeliveryTime)
      {
         ref.setScheduledDeliveryTime(scheduledDeliveryTime);
      }

      public void setPersistedCount(int count)
      {
         ref.setPersistedCount(count);
      }

      public int getPersistedCount()
      {
         return ref.getPersistedCount();
      }

      public boolean isPaged()
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.server.MessageReference#acknowledge(org.apache.activemq.artemis.core.server.MessageReference)
       */
      @Override
      public void acknowledge() throws Exception
      {
         ref.getQueue().acknowledge(this);
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.server.MessageReference#getMessageMemoryEstimate()
       */
      public int getMessageMemoryEstimate()
      {
         return ref.getMessage().getMemoryEstimate();
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.server.MessageReference#setConsumerId(java.lang.Long)
       */
      @Override
      public void setConsumerId(Long consumerID)
      {
         this.consumerId = consumerID;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.server.MessageReference#getConsumerId()
       */
      @Override
      public Long getConsumerId()
      {
         return this.consumerId;
      }
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((map == null) ? 0 : map.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
      {
         return true;
      }
      if (!super.equals(obj))
      {
         return false;
      }
      if (!(obj instanceof LastValueQueue))
      {
         return false;
      }
      LastValueQueue other = (LastValueQueue)obj;
      if (map == null)
      {
         if (other.map != null)
         {
            return false;
         }
      }
      else if (!map.equals(other.map))
      {
         return false;
      }
      return true;
   }
}
