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
package org.apache.activemq.systest.impl;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.systest.AgentStopper;
import org.apache.activemq.systest.AgentSupport;
import org.apache.activemq.systest.BrokerAgent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.ConnectionFactory;

/**
 * A simple in-memory broker implementation
 * 
 * @version $Revision: 1.1 $
 */
public class BrokerAgentImpl extends AgentSupport implements BrokerAgent {
    private static final Log log = LogFactory.getLog(BrokerAgentImpl.class);

    private static int counter;
    private static int port = 61616;

    private BrokerService broker;
    private String brokerName;
    private boolean persistent;
    private String connectionURI;
    private boolean started;
    private boolean deleteAllMessage=true;

    public BrokerAgentImpl() throws Exception {
        brokerName = "broker-" + (++counter);
        connectionURI = "tcp://localhost:" + (port++);
        
        log.info("Creating broker on URI: " + getConnectionURI());
    }

    public void kill() throws Exception {
        stop();
    }

    public ConnectionFactory getConnectionFactory() {
        return new ActiveMQConnectionFactory(getConnectionURI());
    }

    public String getConnectionURI() {
        return connectionURI;
    }

    public void connectTo(BrokerAgent remoteBroker) throws Exception {
        String remoteURI = "static://"+remoteBroker.getConnectionURI();
        log.info("Broker is connecting to network using: " + remoteURI);
        NetworkConnector connector = getBroker().addNetworkConnector(remoteURI);
        if (started) {
            connector.start();
        }
    }

    public void start() throws Exception {
        started = true;
        getBroker().start();
    }

    public void stop(AgentStopper stopper) {
        started = false;
        if (broker != null) {
            try {
                broker.stop();
            }
            catch (Exception e) {
                stopper.onException(this, e);
            }
            finally {
                broker = null;
            }
        }
    }

    public boolean isPersistent() {
        return persistent;
    }

    
    public boolean isStarted() {
        return started;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public BrokerService getBroker() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        return broker;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setBrokerName(brokerName);
        answer.setPersistent(isPersistent());
        
        // Delete all the message the first time the broker is started.
        answer.setDeleteAllMessagesOnStartup(deleteAllMessage);
        deleteAllMessage=false;
        
        answer.addConnector(getConnectionURI());
        return answer;
    }

    public String getBrokerName() {
        return brokerName;
    }
    
    
}
