/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.LRUCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jndi.JndiTemplate;
import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * This bridge joins the gap between foreign JMS providers and ActiveMQ As some JMS providers are still only 1.0.1
 * compliant, this bridge itself aimed to be JMS 1.0.2 compliant.
 * 
 * @version $Revision: 1.1.1.1 $
 */
public abstract class JmsConnector implements Service{
    private static final Log log=LogFactory.getLog(JmsConnector.class);
    protected JndiTemplate jndiTemplate;
    protected JmsMesageConvertor jmsMessageConvertor;
    private List inboundBridges = new CopyOnWriteArrayList();
    private List outboundBridges = new CopyOnWriteArrayList();
    protected int replyToDestinationCacheSize=10000;
    protected AtomicBoolean initialized = new AtomicBoolean(false);
    protected AtomicBoolean started = new AtomicBoolean(false);
    protected ActiveMQConnectionFactory  embeddedConnectionFactory;
    protected LRUCache replyToBridges=new LRUCache(){
        protected boolean removeEldestEntry(Map.Entry enty){
            if(size()>maxCacheSize){
                Iterator iter=entrySet().iterator();
                Map.Entry lru=(Map.Entry) iter.next();
                remove(lru.getKey());
                DestinationBridge bridge=(DestinationBridge) lru.getValue();
                try{
                    bridge.stop();
                    log.info("Expired bridge: "+bridge);
                }catch(Exception e){
                    log.warn("stopping expired bridge"+bridge+" caused an exception",e);
                }
            }
            return false;
        }
    };

    public boolean init(){
        boolean result=initialized.compareAndSet(false,true);
        if(result){
            if(jndiTemplate==null){
                jndiTemplate=new JndiTemplate();
            }
            if(jmsMessageConvertor==null){
                jmsMessageConvertor=new SimpleJmsMessageConvertor();
            }
            replyToBridges.setMaxCacheSize(getReplyToDestinationCacheSize());
        }
        return result;
    }
    
    public void start() throws Exception{
        init();
        if (started.compareAndSet(false, true)){
        for(int i=0;i<inboundBridges.size();i++){
            DestinationBridge bridge=(DestinationBridge) inboundBridges.get(i);
            bridge.start();
        }
        for(int i=0;i<outboundBridges.size();i++){
            DestinationBridge bridge=(DestinationBridge) outboundBridges.get(i);
            bridge.start();
        }
        }
    }

    public void stop() throws Exception{
        if(started.compareAndSet(true,false)){
            for(int i=0;i<inboundBridges.size();i++){
                DestinationBridge bridge=(DestinationBridge) inboundBridges.get(i);
                bridge.stop();
            }
            for(int i=0;i<outboundBridges.size();i++){
                DestinationBridge bridge=(DestinationBridge) outboundBridges.get(i);
                bridge.stop();
            }
        }
    }
    
    /**
     * One way to configure the local connection - this is called by
     * The BrokerService when the Connector is embedded
     * @param service
     */
    public void setBrokerService(BrokerService service){
        embeddedConnectionFactory = new ActiveMQConnectionFactory(service.getVmConnectorURI());
    }

    /**
     * @return Returns the jndiTemplate.
     */
    public JndiTemplate getJndiTemplate(){
        return jndiTemplate;
    }

    /**
     * @param jndiTemplate
     *            The jndiTemplate to set.
     */
    public void setJndiTemplate(JndiTemplate jndiTemplate){
        this.jndiTemplate=jndiTemplate;
    }

    /**
     * @return Returns the jmsMessageConvertor.
     */
    public JmsMesageConvertor getJmsMessageConvertor(){
        return jmsMessageConvertor;
    }

    /**
     * @param jmsMessageConvertor
     *            The jmsMessageConvertor to set.
     */
    public void setJmsMessageConvertor(JmsMesageConvertor jmsMessageConvertor){
        this.jmsMessageConvertor=jmsMessageConvertor;
    }

    /**
     * @return Returns the replyToDestinationCacheSize.
     */
    public int getReplyToDestinationCacheSize(){
        return replyToDestinationCacheSize;
    }

    /**
     * @param replyToDestinationCacheSize
     *            The replyToDestinationCacheSize to set.
     */
    public void setReplyToDestinationCacheSize(int replyToDestinationCacheSize){
        this.replyToDestinationCacheSize=replyToDestinationCacheSize;
    }
    
    
    protected void addInboundBridge(DestinationBridge bridge){
        inboundBridges.add(bridge);
    }
    
    protected void addOutboundBridge(DestinationBridge bridge){
        outboundBridges.add(bridge);
    }
    protected void removeInboundBridge(DestinationBridge bridge){
        inboundBridges.add(bridge);
    }
    
    protected void removeOutboundBridge(DestinationBridge bridge){
        outboundBridges.add(bridge);
    }
}