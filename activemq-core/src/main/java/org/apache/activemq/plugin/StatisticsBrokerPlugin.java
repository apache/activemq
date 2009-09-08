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
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A StatisticsBrokerPlugin
 * @org.apache.xbean.XBean element="statisticsBrokerPlugin"
 *
 */
public class StatisticsBrokerPlugin implements BrokerPlugin {
    private static Log LOG = LogFactory.getLog(StatisticsBrokerPlugin.class);
    /** 
     * @param broker
     * @return the plug-in
     * @throws Exception
     * @see org.apache.activemq.broker.BrokerPlugin#installPlugin(org.apache.activemq.broker.Broker)
     */
    public Broker installPlugin(Broker broker) throws Exception {
        StatisticsBroker answer = new StatisticsBroker(broker);
        LOG.info("Installing StaticsBroker");
        return answer;
    }
}
