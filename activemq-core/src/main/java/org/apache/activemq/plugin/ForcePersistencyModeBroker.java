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
package org.apache.activemq.plugin;


import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * A Plugin which allows to force every incoming message to be PERSISTENT or NON-PERSISTENT. 
 * 
 * Useful, if you have set the broker usage policy to process ONLY persistent or ONLY non-persistent
 * messages. 
 * @org.apache.xbean.XBean element="forcePersistencyModeBroker"
 */
public class ForcePersistencyModeBroker extends BrokerFilter{
  public static Log log = LogFactory.getLog(ForcePersistencyModeBroker.class);
  private boolean persistence = false;
  
  /**
   * @return the persistenceFlag
   */
  public boolean isPersistent() {
    return persistence;
  }

  /**
   * @param persistenceFlag the persistenceFlag to set
   */
  public void setPersistenceFlag(boolean mode) {
    this.persistence = mode;
  }

  /**
   * Constructor
   * @param next
   */
  public ForcePersistencyModeBroker(Broker next) {
    super(next);
    System.out.println(this.getBrokerSequenceId());
  }
  
  /** Sets the persistence mode
   * @see org.apache.activemq.broker.BrokerFilter#send(org.apache.activemq.broker.ProducerBrokerExchange, org.apache.activemq.command.Message)
   */
  public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
    messageSend.getMessage().setPersistent(isPersistent());
    next.send(producerExchange, messageSend);
  }
  
}

