/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.broker.jmx;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.OpenTypeSupport.OpenTypeFactory;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;

public class DestinationView {
    protected final Destination destination;
    protected final ManagedRegionBroker broker;

    public DestinationView(ManagedRegionBroker broker, Destination destination){
        this.broker = broker;
        this.destination=destination;
    }

    public void gc(){
        destination.gc();
    }

    public void resetStatistics(){
        destination.getDestinationStatistics().reset();
    }

    public long getEnqueueCount(){
        return destination.getDestinationStatistics().getEnqueues().getCount();
    }

    public long getDequeueCount(){
        return destination.getDestinationStatistics().getDequeues().getCount();
    }

    public long getConsumerCount(){
        return destination.getDestinationStatistics().getConsumers().getCount();
    }

    public long getQueueSize(){
        return destination.getDestinationStatistics().getMessages().getCount();
    }

    public long getMessagesCached(){
        return destination.getDestinationStatistics().getMessagesCached().getCount();
    }

    public CompositeData[] browse() throws OpenDataException{
        Message[] messages=destination.browse();
        CompositeData c[]=new CompositeData[messages.length];
        for(int i=0;i<c.length;i++){
            try{
                c[i]=OpenTypeSupport.convert(messages[i]);
            }catch(Throwable e){
                e.printStackTrace();
            }
        }
        return c;
    }

    public TabularData browseAsTable() throws OpenDataException{
        OpenTypeFactory factory=OpenTypeSupport.getFactory(ActiveMQMessage.class);
        Message[] messages=destination.browse();
        CompositeType ct=factory.getCompositeType();
        TabularType tt=new TabularType("MessageList","MessageList",ct,new String[] { "JMSMessageID" });
        TabularDataSupport rc=new TabularDataSupport(tt);
        for(int i=0;i<messages.length;i++){
            rc.put(new CompositeDataSupport(ct,factory.getFields(messages[i])));
        }
        return rc;
    }
    
    public String sendTextMessage(String body) throws Exception {
    	return sendTextMessage(Collections.EMPTY_MAP, body);
    }
    
    public String sendTextMessage(Map headers, String body) throws Exception {
    	
    	String brokerUrl = "vm://"+broker.getBrokerName();
    	ActiveMQDestination dest = destination.getActiveMQDestination();
    	
    	ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
    	Connection connection = null;
    	try {
    		
    		connection = cf.createConnection();
			Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
			MessageProducer producer = session.createProducer(dest);
			ActiveMQTextMessage msg = (ActiveMQTextMessage) session.createTextMessage(body);
			
			for (Iterator iter = headers.entrySet().iterator(); iter.hasNext();) {
				Map.Entry entry = (Map.Entry) iter.next();
				msg.setObjectProperty((String) entry.getKey(), entry.getValue());
			}
			
			producer.setDeliveryMode(msg.getJMSDeliveryMode());
			producer.setPriority(msg.getPriority());
			long ttl = msg.getExpiration() - System.currentTimeMillis();
			producer.setTimeToLive(ttl > 0 ? ttl : 0);
	    	producer.send(msg);
	    	
	    	return msg.getJMSMessageID();
	    	
    	} finally {
    		connection.close();
    	}
    	
    }

}
