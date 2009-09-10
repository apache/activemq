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

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Filip Hanik
 * @org.apache.xbean.XBean element="discardingDLQBrokerPlugin"
 * @version 1.0
 */
public class DiscardingDLQBrokerPlugin implements BrokerPlugin {
    public DiscardingDLQBrokerPlugin() {
    }

    public static Log log = LogFactory.getLog(DiscardingDLQBrokerPlugin.class);
    private boolean dropTemporaryTopics = true;
    private boolean dropTemporaryQueues = true;
    private boolean dropAll = true;
    private String dropOnly;
    private int reportInterval = 1000;

    /**
     * Installs the plugin into the interceptor chain of the broker, returning the new intercepted broker to use.
     * @param broker Broker
     * @throws Exception
     * @return Broker
     * @todo Implement this org.apache.activemq.broker.BrokerPlugin method
     */
    public Broker installPlugin(Broker broker) throws Exception {
        log.info("Installing Discarding Dead Letter Queue broker plugin[dropAll="+isDropAll()+
                 "; dropTemporaryTopics="+isDropTemporaryTopics()+"; dropTemporaryQueues="+
                 isDropTemporaryQueues()+"; dropOnly="+getDropOnly()+"; reportInterval="+
                 getReportInterval()+"]");
        DiscardingDLQBroker cb = new DiscardingDLQBroker(broker);
        cb.setDropAll(isDropAll());
        cb.setDropTemporaryQueues(isDropTemporaryQueues());
        cb.setDropTemporaryTopics(isDropTemporaryTopics());
        cb.setDestFilter(getDestFilter());
        return cb;
    }

    public boolean isDropAll() {
        return dropAll;
    }

    public boolean isDropTemporaryQueues() {
        return dropTemporaryQueues;
    }

    public boolean isDropTemporaryTopics() {
        return dropTemporaryTopics;
    }

    public String getDropOnly() {
        return dropOnly;
    }

    public int getReportInterval() {
        return reportInterval;
    }

    public void setDropTemporaryTopics(boolean dropTemporaryTopics) {
        this.dropTemporaryTopics = dropTemporaryTopics;
    }

    public void setDropTemporaryQueues(boolean dropTemporaryQueues) {
        this.dropTemporaryQueues = dropTemporaryQueues;
    }

    public void setDropAll(boolean dropAll) {
        this.dropAll = dropAll;
    }

    public void setDropOnly(String dropOnly) {
        this.dropOnly = dropOnly;
    }

    public void setReportInterval(int reportInterval) {
        this.reportInterval = reportInterval;
    }

    public Pattern[] getDestFilter() {
        if (getDropOnly()==null) return null;
        ArrayList<Pattern> list = new ArrayList<Pattern>();
        StringTokenizer t = new StringTokenizer(getDropOnly()," ");
        while (t.hasMoreTokens()) {
            String s = t.nextToken();
            if (s!=null && s.trim().length()>0) list.add(Pattern.compile(s));
        }
        if (list.size()==0) return null;
        return list.toArray(new Pattern[0]);
    }
}
