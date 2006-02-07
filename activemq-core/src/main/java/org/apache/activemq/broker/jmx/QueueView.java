/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.broker.jmx;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.activemq.broker.jmx.OpenTypeSupport.OpenTypeFactory;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.Message;

public class QueueView implements QueueViewMBean {

    private final Queue destination;

    public QueueView(Queue destination) {
        this.destination = destination;
    }

    public void gc() {
        destination.gc();
    }
    public void resetStatistics() {
        destination.getDestinationStatistics().reset();
    }

    public long getEnqueueCount() {
        return destination.getDestinationStatistics().getEnqueues().getCount();
    
    }
    public long getDequeueCount() {
        return destination.getDestinationStatistics().getDequeues().getCount();
    }

    public long getConsumerCount() {
        return destination.getDestinationStatistics().getConsumers().getCount();
    }
    
    public long getMessages() {
        return destination.getDestinationStatistics().getMessages().getCount();
    }
    
    public long getMessagesCached() {
        return destination.getDestinationStatistics().getMessagesCached().getCount();
    }
    
    public CompositeData[] browse() throws OpenDataException {
        Message[] messages = destination.browse();
        CompositeData c[] = new CompositeData[messages.length];
        for (int i = 0; i < c.length; i++) {
            try {
                System.out.println(messages[i].getMessageId());
                c[i] = OpenTypeSupport.convert(messages[i]);
            } catch (Throwable e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return c;
    }
    
    public TabularData browseAsTable() throws OpenDataException {
        OpenTypeFactory factory = OpenTypeSupport.getFactory(ActiveMQMessage.class);
        
        Message[] messages = destination.browse();
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("MessageList", "MessageList", ct, new String[]{"JMSMessageID"});
        TabularDataSupport rc = new TabularDataSupport(tt);
        for (int i = 0; i < messages.length; i++) {
            System.out.println(messages[i].getMessageId());
            rc.put(new CompositeDataSupport(ct, factory.getFields(messages[i])));
        }
        
        return rc;
    }

    
    public CompositeData getMessage(String messageId) throws OpenDataException {
        Message rc = destination.getMessage(messageId);
        if( rc ==null )
            return null;
        return OpenTypeSupport.convert(rc);
    }
    
    public void removeMessage(String messageId) {
        destination.removeMessage(messageId);
    }

    public void purge() {
        destination.purge();
    }
    
}
