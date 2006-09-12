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
import java.util.LinkedList;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * perist pending messages pending message (messages awaiting disptach to a consumer) cursor
 * 
 * @version $Revision$
 */
class TopicStorePrefetch extends AbstractPendingMessageCursor implements MessageRecoveryListener{
    static private final Log log=LogFactory.getLog(TopicStorePrefetch.class);
    private Topic topic;
    private TopicMessageStore store;
    private LinkedList batchList;
    private String clientId;
    private String subscriberName;
    private int pendingCount=0;
    private MessageId lastMessageId;
    private int maxBatchSize=10;

    /**
     * @param topic
     * @param batchList
     * @param clientId
     * @param subscriberName
     * @throws IOException
     */
    public TopicStorePrefetch(Topic topic,LinkedList batchList,String clientId,String subscriberName){
        this.topic=topic;
        this.store=(TopicMessageStore) topic.getMessageStore();
        this.batchList=batchList;
        this.clientId=clientId;
        this.subscriberName=subscriberName;
    }

    public void start() throws Exception{
        pendingCount=store.getMessageCount(clientId,subscriberName);
        System.err.println("Pending count = "+pendingCount);
    }

    public void stop() throws Exception{
        pendingCount=0;
        lastMessageId=null;
    }

    /**
     * @return true if there are no pending messages
     */
    public boolean isEmpty(){
        return pendingCount==0;
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
        lastMessageId=null;
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
                    log.error(topic.getDestination()+" Couldn't fill batch from store ",e);
                    throw new RuntimeException(e);
                }
            }
            result=(MessageReference) batchList.removeFirst();
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

    // MessageRecoveryListener implementation
    public void finished(){}

    public void recoverMessage(Message message) throws Exception{
        batchList.addLast(message);
    }

    public void recoverMessageReference(String messageReference) throws Exception{
        // shouldn't get called
        throw new RuntimeException("Not supported");
    }

    // implementation
    protected void fillBatch() throws Exception{
        if(pendingCount<=0){
            pendingCount=store.getMessageCount(clientId,subscriberName);
        }
        if(pendingCount>0){
            store.recoverNextMessages(clientId,subscriberName,lastMessageId,maxBatchSize,this);
            // this will add more messages to the batch list
            if(!batchList.isEmpty()){
                Message message=(Message) batchList.getLast();
                lastMessageId=message.getMessageId();
            }
        }
    }
}
