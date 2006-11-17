/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.broker.region.cursors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.Message;
import org.apache.activemq.kaha.Store;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * perist pending messages pending message (messages awaiting disptach to a consumer) cursor
 * 
 * @version $Revision$
 */
public class StoreDurableSubscriberCursor extends AbstractPendingMessageCursor{

    static private final Log log=LogFactory.getLog(StoreDurableSubscriberCursor.class);
    private int pendingCount=0;
    private String clientId;
    private String subscriberName;
    private Map topics=new HashMap();
    private LinkedList storePrefetches=new LinkedList();
    private boolean started;
    private PendingMessageCursor nonPersistent;
    private PendingMessageCursor currentCursor;

    /**
     * @param topic
     * @param clientId
     * @param subscriberName
     * @throws IOException
     */
    public StoreDurableSubscriberCursor(String clientId,String subscriberName,Store store,int maxBatchSize){
        this.clientId=clientId;
        this.subscriberName=subscriberName;
        this.nonPersistent=new FilePendingMessageCursor(clientId+subscriberName,store);
        storePrefetches.add(nonPersistent);
    }

    public synchronized void start() throws Exception{
        started=true;
        for(Iterator i=storePrefetches.iterator();i.hasNext();){
            PendingMessageCursor tsp=(PendingMessageCursor)i.next();
            tsp.start();
            pendingCount+=tsp.size();
        }
    }

    public synchronized void stop() throws Exception{
        started=false;
        for(Iterator i=storePrefetches.iterator();i.hasNext();){
            PendingMessageCursor tsp=(PendingMessageCursor)i.next();
            tsp.stop();
        }
        pendingCount=0;
    }

    /**
     * Add a destination
     * 
     * @param context
     * @param destination
     * @throws Exception
     */
    public synchronized void add(ConnectionContext context,Destination destination) throws Exception{
        TopicStorePrefetch tsp=new TopicStorePrefetch((Topic)destination,clientId,subscriberName);
        tsp.setMaxBatchSize(getMaxBatchSize());
        topics.put(destination,tsp);
        storePrefetches.add(tsp);
        if(started){
            tsp.start();
            pendingCount+=tsp.size();
        }
    }

    /**
     * remove a destination
     * 
     * @param context
     * @param destination
     * @throws Exception
     */
    public synchronized void remove(ConnectionContext context,Destination destination) throws Exception{
        Object tsp=topics.remove(destination);
        if(tsp!=null){
            storePrefetches.remove(tsp);
        }
    }

    /**
     * @return true if there are no pending messages
     */
    public synchronized boolean isEmpty(){
        return pendingCount<=0;
    }

    /**
     * Informs the Broker if the subscription needs to intervention to recover it's state e.g. DurableTopicSubscriber
     * may do
     * 
     * @see org.apache.activemq.region.cursors.PendingMessageCursor
     * @return true if recovery required
     */
    public boolean isRecoveryRequired(){
        return false;
    }

    
    public synchronized void addMessageLast(MessageReference node) throws Exception{
        if(node!=null){
            Message msg=node.getMessage();
            if(started){
                pendingCount++;
                if(!msg.isPersistent()){
                    nonPersistent.addMessageLast(node);
                }
            }
            if(msg.isPersistent()){
                Destination dest=msg.getRegionDestination();
                TopicStorePrefetch tsp=(TopicStorePrefetch)topics.get(dest);
                if(tsp!=null){
                    tsp.addMessageLast(node);
                }
            }
        }
    }

    public void clear(){
        pendingCount=0;
    }

    public synchronized boolean hasNext(){
        boolean result=pendingCount>0;
        if(result){
            try{
                currentCursor=getNextCursor();
            }catch(Exception e){
                log.error("Failed to get current cursor ",e);
                throw new RuntimeException(e);
            }
            result=currentCursor!=null?currentCursor.hasNext():false;
        }
        return result;
    }

    public synchronized MessageReference next(){
        return currentCursor!=null?currentCursor.next():null;
    }

    public synchronized void remove(){
        if(currentCursor!=null){
            currentCursor.remove();
        }
        pendingCount--;
    }
    
    public void remove(MessageReference node){
        if(currentCursor!=null){
            currentCursor.remove(node);
        }
        pendingCount--;
    }

    public synchronized void reset(){
        nonPersistent.reset();
        for(Iterator i=storePrefetches.iterator();i.hasNext();){
            AbstractPendingMessageCursor tsp=(AbstractPendingMessageCursor)i.next();
            tsp.reset();
        }
    }

    public int size(){
        return pendingCount;
    }
    
    public synchronized void setMaxBatchSize(int maxBatchSize){
        for(Iterator i=storePrefetches.iterator();i.hasNext();){
            AbstractPendingMessageCursor tsp=(AbstractPendingMessageCursor)i.next();
            tsp.setMaxBatchSize(maxBatchSize);
        }
        super.setMaxBatchSize(maxBatchSize);
    }

    protected synchronized PendingMessageCursor getNextCursor() throws Exception{
        if(currentCursor==null||currentCursor.isEmpty()){
            currentCursor=null;
            for(Iterator i=storePrefetches.iterator();i.hasNext();){
                AbstractPendingMessageCursor tsp=(AbstractPendingMessageCursor)i.next(); 
                if(tsp.hasNext()){
                    currentCursor=tsp;
                    break;
                }
            }
            // round-robin
            storePrefetches.addLast(storePrefetches.removeFirst());
        }
        return currentCursor;
    }
}
