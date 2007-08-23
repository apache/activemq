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
package org.apache.activemq.web;

import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;

/**
 * A facade for the broker in the same JVM and ClassLoader
 * 
 * @version $Revision$
 */
public class SingletonBrokerFacade extends LocalBrokerFacade {
    public SingletonBrokerFacade() {
        super(findSingletonBroker());
    }

    protected static BrokerService findSingletonBroker() {
        BrokerService broker = BrokerRegistry.getInstance().findFirst();
        if (broker == null) {
            throw new IllegalArgumentException("No BrokerService is registered with the BrokerRegistry. Are you sure there is a configured broker in the same ClassLoader?");
        }
        return broker;
    }
}
