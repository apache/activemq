/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.ra;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;

import org.apache.activemq.ActiveMQXAConnectionFactory;

/**
 * Impl that will create an XAConnectionFactory that allows 
 * org.jboss.jms.server.recovery.MessagingXAResourceRecovery to deal with recovery via jms apis
 * on the connectionfactory registered in jndi 
 * to use, reference in the ra.xml as follows:
 * {@code <managedconnectionfactory-class>org.apache.activemq.ra.ActiveMQXAManagedConnectionFactory</managedconnectionfactory-class>}
 *               
 */
public class ActiveMQXAManagedConnectionFactory extends ActiveMQManagedConnectionFactory {

   private static final long serialVersionUID = 2032273395660212918L;

   /**
    * This method returns an ActiveMQXAConnectonFactory<br/>
    * 
    * @see javax.resource.spi.ManagedConnectionFactory#createConnectionFactory(javax.resource.spi.ConnectionManager)
    */
   @Override
   public Object createConnectionFactory(ConnectionManager manager) throws ResourceException {
      return new ActiveMQXAConnectionFactory( getInfo().getUserName(), getInfo().getPassword(), getInfo().getServerUrl());
   }
   
   /**
    * @see java.lang.Object#equals(java.lang.Object)
    */
   @Override
   public boolean equals(Object object) {
      if (object == null || object.getClass() != ActiveMQXAManagedConnectionFactory.class) {
          return false;
      }
      return ((ActiveMQXAManagedConnectionFactory)object).getInfo().equals(getInfo());
   }

   /**
    * @see java.lang.Object#hashCode()
    */
   @Override
   public int hashCode() {
      return getInfo().hashCode();
   }
}
