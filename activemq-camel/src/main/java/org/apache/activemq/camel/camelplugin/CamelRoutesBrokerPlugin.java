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

package org.apache.activemq.camel.camelplugin;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A CamelRoutesBrokerPlugin
 *
 * load camel routes dynamically from a routes.xml file located in same directory as ActiveMQ.xml
 *
 * @org.apache.xbean.XBean element="camelRoutesBrokerPlugin"
 *
 */
public class CamelRoutesBrokerPlugin implements BrokerPlugin {
    private static Logger LOG = LoggerFactory.getLogger(CamelRoutesBrokerPlugin.class);
    private String routesFile = "";
    private int checkPeriod =1000;

    public String getRoutesFile() {
        return routesFile;
    }

    public void setRoutesFile(String routesFile) {
        this.routesFile = routesFile;
    }

    public int getCheckPeriod() {
        return checkPeriod;
    }

    public void setCheckPeriod(int checkPeriod) {
        this.checkPeriod = checkPeriod;
    }

    /** 
     * @param broker
     * @return the plug-in
     * @throws Exception
     * @see org.apache.activemq.broker.BrokerPlugin#installPlugin(org.apache.activemq.broker.Broker)
     */
    public Broker installPlugin(Broker broker) throws Exception {
        CamelRoutesBroker answer = new CamelRoutesBroker(broker);
        answer.setCheckPeriod(getCheckPeriod());
        answer.setRoutesFile(getRoutesFile());
        LOG.info("Installing CamelRoutesBroker");
        return answer;
    }
}
