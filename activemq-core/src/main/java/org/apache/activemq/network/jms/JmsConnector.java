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
package org.apache.activemq.network.jms;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.NamingException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.LRUCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jndi.JndiTemplate;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This bridge joins the gap between foreign JMS providers and ActiveMQ As some
 * JMS providers are still only 1.0.1 compliant, this bridge itself aimed to be
 * JMS 1.0.2 compliant.
 * 
 * @version $Revision: 1.1.1.1 $
 */
public abstract class JmsConnector implements Service {

    private static final Log log = LogFactory.getLog(JmsConnector.class);
    protected JndiTemplate jndiLocalTemplate;
    protected JndiTemplate jndiOutboundTemplate;
    protected JmsMesageConvertor inboundMessageConvertor;
    protected JmsMesageConvertor outboundMessageConvertor;
    private List inboundBridges = new CopyOnWriteArrayList();
    private List outboundBridges = new CopyOnWriteArrayList();
    protected AtomicBoolean initialized = new AtomicBoolean(false);
    protected AtomicBoolean started = new AtomicBoolean(false);
    protected ActiveMQConnectionFactory embeddedConnectionFactory;
    protected int replyToDestinationCacheSize = 10000;
    protected String outboundUsername;
    protected String outboundPassword;
    protected String localUsername;
    protected String localPassword;
    private String name;

    protected LRUCache replyToBridges = createLRUCache(); 
    	
    static private LRUCache createLRUCache() { 
    	return new LRUCache() {
	        private static final long serialVersionUID = -7446792754185879286L;
	
	        protected boolean removeEldestEntry(Map.Entry enty) {
	            if (size() > maxCacheSize) {
	                Iterator iter = entrySet().iterator();
	                Map.Entry lru = (Map.Entry) iter.next();
	                remove(lru.getKey());
	                DestinationBridge bridge = (DestinationBridge) lru.getValue();
	                try {
	                    bridge.stop();
	                    log.info("Expired bridge: " + bridge);
	                }
	                catch (Exception e) {
	                    log.warn("stopping expired bridge" + bridge + " caused an exception", e);
	                }
	            }
	            return false;
	        }
	    };
    }

    /**
     */
    public boolean init() {
        boolean result = initialized.compareAndSet(false, true);
        if (result) {
            if (jndiLocalTemplate == null) {
                jndiLocalTemplate = new JndiTemplate();
            }
            if (jndiOutboundTemplate == null) {
                jndiOutboundTemplate = new JndiTemplate();
            }
            if (inboundMessageConvertor == null) {
                inboundMessageConvertor = new SimpleJmsMessageConvertor();
            }
            if (outboundMessageConvertor == null) {
                outboundMessageConvertor = new SimpleJmsMessageConvertor();
            }
            replyToBridges.setMaxCacheSize(getReplyToDestinationCacheSize());
        }
        return result;
    }

    public void start() throws Exception {
        init();
        if (started.compareAndSet(false, true)) {
            for (int i = 0; i < inboundBridges.size(); i++) {
                DestinationBridge bridge = (DestinationBridge) inboundBridges.get(i);
                bridge.start();
            }
            for (int i = 0; i < outboundBridges.size(); i++) {
                DestinationBridge bridge = (DestinationBridge) outboundBridges.get(i);
                bridge.start();
            }
            log.info("JMS Connector " + getName() + " Started");
        }
    }

    public void stop() throws Exception {
        if (started.compareAndSet(true, false)) {
            for (int i = 0; i < inboundBridges.size(); i++) {
                DestinationBridge bridge = (DestinationBridge) inboundBridges.get(i);
                bridge.stop();
            }
            for (int i = 0; i < outboundBridges.size(); i++) {
                DestinationBridge bridge = (DestinationBridge) outboundBridges.get(i);
                bridge.stop();
            }
            log.info("JMS Connector " + getName() + " Stopped");
        }
    }

    protected abstract Destination createReplyToBridge(Destination destination, Connection consumerConnection, Connection producerConnection);

    /**
     * One way to configure the local connection - this is called by The
     * BrokerService when the Connector is embedded
     * 
     * @param service
     */
    public void setBrokerService(BrokerService service) {
        embeddedConnectionFactory = new ActiveMQConnectionFactory(service.getVmConnectorURI());
    }

    /**
     * @return Returns the jndiTemplate.
     */
    public JndiTemplate getJndiLocalTemplate() {
        return jndiLocalTemplate;
    }

    /**
     * @param jndiTemplate
     *            The jndiTemplate to set.
     */
    public void setJndiLocalTemplate(JndiTemplate jndiTemplate) {
        this.jndiLocalTemplate = jndiTemplate;
    }

    /**
     * @return Returns the jndiOutboundTemplate.
     */
    public JndiTemplate getJndiOutboundTemplate() {
        return jndiOutboundTemplate;
    }

    /**
     * @param jndiOutboundTemplate
     *            The jndiOutboundTemplate to set.
     */
    public void setJndiOutboundTemplate(JndiTemplate jndiOutboundTemplate) {
        this.jndiOutboundTemplate = jndiOutboundTemplate;
    }

    /**
     * @return Returns the inboundMessageConvertor.
     */
    public JmsMesageConvertor getInboundMessageConvertor() {
        return inboundMessageConvertor;
    }

    /**
     * @param inboundMessageConvertor
     *            The inboundMessageConvertor to set.
     */
    public void setInboundMessageConvertor(JmsMesageConvertor jmsMessageConvertor) {
        this.inboundMessageConvertor = jmsMessageConvertor;
    }

    /**
     * @return Returns the outboundMessageConvertor.
     */
    public JmsMesageConvertor getOutboundMessageConvertor() {
        return outboundMessageConvertor;
    }

    /**
     * @param outboundMessageConvertor
     *            The outboundMessageConvertor to set.
     */
    public void setOutboundMessageConvertor(JmsMesageConvertor outboundMessageConvertor) {
        this.outboundMessageConvertor = outboundMessageConvertor;
    }

    /**
     * @return Returns the replyToDestinationCacheSize.
     */
    public int getReplyToDestinationCacheSize() {
        return replyToDestinationCacheSize;
    }

    /**
     * @param replyToDestinationCacheSize
     *            The replyToDestinationCacheSize to set.
     */
    public void setReplyToDestinationCacheSize(int replyToDestinationCacheSize) {
        this.replyToDestinationCacheSize = replyToDestinationCacheSize;
    }

    /**
     * @return Returns the localPassword.
     */
    public String getLocalPassword() {
        return localPassword;
    }

    /**
     * @param localPassword
     *            The localPassword to set.
     */
    public void setLocalPassword(String localPassword) {
        this.localPassword = localPassword;
    }

    /**
     * @return Returns the localUsername.
     */
    public String getLocalUsername() {
        return localUsername;
    }

    /**
     * @param localUsername
     *            The localUsername to set.
     */
    public void setLocalUsername(String localUsername) {
        this.localUsername = localUsername;
    }

    /**
     * @return Returns the outboundPassword.
     */
    public String getOutboundPassword() {
        return outboundPassword;
    }

    /**
     * @param outboundPassword
     *            The outboundPassword to set.
     */
    public void setOutboundPassword(String outboundPassword) {
        this.outboundPassword = outboundPassword;
    }

    /**
     * @return Returns the outboundUsername.
     */
    public String getOutboundUsername() {
        return outboundUsername;
    }

    /**
     * @param outboundUsername
     *            The outboundUsername to set.
     */
    public void setOutboundUsername(String outboundUsername) {
        this.outboundUsername = outboundUsername;
    }

    protected void addInboundBridge(DestinationBridge bridge) {
        inboundBridges.add(bridge);
    }

    protected void addOutboundBridge(DestinationBridge bridge) {
        outboundBridges.add(bridge);
    }

    protected void removeInboundBridge(DestinationBridge bridge) {
        inboundBridges.add(bridge);
    }

    protected void removeOutboundBridge(DestinationBridge bridge) {
        outboundBridges.add(bridge);
    }

    public String getName() {
        if (name == null) {
            name = "Connector:" + getNextId();
        }
        return name;
    }

    static int nextId;

    static private synchronized int getNextId() {
        return nextId++;
    }

    public void setName(String name) {
        this.name = name;
    }

    public abstract void restartProducerConnection() throws NamingException, JMSException;
}
