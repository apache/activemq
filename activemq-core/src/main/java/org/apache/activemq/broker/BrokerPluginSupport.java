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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A useful base class for implementing broker plugins.
 * 
 * @version $Revision$
 */
public abstract class BrokerPluginSupport extends MutableBrokerFilter implements BrokerPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerPluginSupport.class);
    public BrokerPluginSupport() {
        super(null);
    }

    public Broker installPlugin(Broker broker) throws Exception {
        setNext(broker);
        return this;
    }
    
    @Override
    public void start() throws Exception {
        super.start();
        LOG.info("Broker Plugin " + getClass().getName() + " started");
    }
    
    @Override
    public void stop() throws Exception {
        super.stop();
        LOG.info("Broker Plugin " + getClass().getName() + " stopped");
    }
    
}
