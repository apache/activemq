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
package org.apache.activemq.broker;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.3 $
 */
public class BrokerRegistry {

    private static final Log LOG = LogFactory.getLog(BrokerRegistry.class);
    private static final BrokerRegistry INSTANCE = new BrokerRegistry();

    private final Object mutex = new Object();
    private final Map<String, BrokerService> brokers = new HashMap<String, BrokerService>();

    public static BrokerRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * @param brokerName
     * @return the BrokerService
     */
    public BrokerService lookup(String brokerName) {
        BrokerService result = null;
        synchronized (mutex) {
            result = brokers.get(brokerName);
            if (result == null && brokerName != null && brokerName.equals(BrokerService.DEFAULT_BROKER_NAME)) {
                result = findFirst();
                if (result != null) {
                    LOG.warn("Broker localhost not started so using " + result.getBrokerName() + " instead");
                }
            }
        }
        return result;
    }

    /**
     * Returns the first registered broker found
     * 
     * @return the first BrokerService
     */
    public BrokerService findFirst() {
        synchronized (mutex) {
            Iterator<BrokerService> iter = brokers.values().iterator();
            while (iter.hasNext()) {
                return iter.next();
            }
            return null;
        }
    }

    /**
     * @param brokerName
     * @param broker
     */
    public void bind(String brokerName, BrokerService broker) {
        synchronized (mutex) {
            brokers.put(brokerName, broker);
            mutex.notifyAll();
        }
    }

    /**
     * @param brokerName
     */
    public void unbind(String brokerName) {
        synchronized (mutex) {
            brokers.remove(brokerName);
        }
    }

    /**
     * @return the mutex used
     */
    public Object getRegistryMutext() {
        return mutex;
    }
}
