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

package org.apache.activemq.i18n;

import java.lang.reflect.Field;
import java.security.PrivilegedAction;
import java.util.Locale;

import static java.security.AccessController.doPrivileged;

public class I18NFactory
{
   public static <T> T getMessageBundle(Class<T> type)
   {
      T instance = getMessageBundle(type, Locale.getDefault());

      if (instance == null)
      {
         instance = getMessageBundle(type, null);
      }

      if (instance == null)
      {
         throw new NullPointerException("couldn't find MessageBundle for " + type);
      }

      return instance;
   }


   public static <T> T getMessageBundle(final Class<T> type, final Locale locale)
   {
      return doPrivileged(new PrivilegedAction<T>()
      {
         public T run()
         {
            String language = "";

            if (locale != null)
            {
               language = locale.getLanguage();
            }

            try
            {
               String className = type.getName() + "_impl" + language;
               System.out.println("Loading [" + className + "]");
               Class messageClass = Class.forName(className, true, type.getClassLoader()).asSubclass(type);

               Field field = messageClass.getField("INSTANCE");

               return type.cast(field.get(null));
            }
            catch (ClassNotFoundException e)
            {
               return null;
            }
            catch (NoSuchFieldException e)
            {
               throw new IllegalStateException(e.getMessage(), e);
            }
            catch (IllegalAccessException e)
            {
               e.printStackTrace();
               throw new IllegalStateException(e.getMessage(), e);
            }
         }
      });
   }

   public static <T> T getMessageLogger(Class<T> type)
   {
      return getMessageBundle(type);
   }

}
