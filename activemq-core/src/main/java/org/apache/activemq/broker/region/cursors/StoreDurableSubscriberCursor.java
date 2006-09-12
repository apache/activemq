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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;
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
    private int maxBatchSize=10;
    private LinkedList batchList=new LinkedList();
    private Map topics=new HashMap();
    private LinkedList storePrefetches=new LinkedList();
    private AtomicBoolean started=new AtomicBoolean();

    /**
     * @param topic
     * @param clientId
     * @param subscriberName
     * @throws IOException
     */
    public StoreDurableSubscriberCursor(String clientId,String subscriberName){
        this.clientId=clientId;
        this.subscriberName=subscriberName;
    }

    public synchronized void start() throws Exception{
        started.set(true);
        for(Iterator i=storePrefetches.iterator();i.hasNext();){
            TopicStorePrefetch tsp=(TopicStorePrefetch) i.next();
            tsp.start();
            pendingCount+=tsp.size();
        }
    }

    public synchronized void stop() throws Exception{
        started.set(false);
        for(Iterator i=storePrefetches.iterator();i.hasNext();){
            TopicStorePrefetch tsp=(TopicStorePrefetch) i.next();
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
        TopicStorePrefetch tsp=new TopicStorePrefetch((Topic) destination,batchList,clientId,subscriberName);
        topics.put(destination,tsp);
        storePrefetches.add(tsp);
        if(started.get()){
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
        TopicStorePrefetch tsp=(TopicStorePrefetch) topics.remove(destination);
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

    public synchronized void addMessageFirst(MessageReference node){
        pendingCount++;
    }

    public synchronized void addMessageLast(MessageReference node){
        pendingCount++;
    }

    public void clear(){
        pendingCount=0;
    }

    public synchronized boolean hasNext(){
        return !isEmpty();
    }

    public synchronized MessageReference next(){
        MessageReference result=null;
        if(!isEmpty()){
            if(batchList.isEmpty()){
                try{
                    fillBatch();
                }catch(Exception e){
                    log.error("Couldn't fill batch from store ",e);
                    throw new RuntimeException(e);
                }
            }
            if(!batchList.isEmpty()){
                result=(MessageReference) batchList.removeFirst();
            }
        }
        return result;
    }

    public synchronized void remove(){
        pendingCount--;
    }

    public void reset(){
        batchList.clear();
    }

    public int size(){
        return pendingCount;
    }

    private synchronized void fillBatch() throws Exception{
        for(Iterator i=storePrefetches.iterator();i.hasNext();){
            TopicStorePrefetch tsp=(TopicStorePrefetch) i.next();
            tsp.fillBatch();
            if(batchList.size()>=maxBatchSize){
                break;
            }
        }
        // round-robin
        Object obj=storePrefetches.removeFirst();
        storePrefetches.addLast(obj);
    }
}
