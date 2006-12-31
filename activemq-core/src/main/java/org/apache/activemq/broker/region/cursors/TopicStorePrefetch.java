/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.activemq.broker.region.cursors;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * perist pending messages pending message (messages awaiting disptach to a
 * consumer) cursor
 * 
 * @version $Revision$
 */
class TopicStorePrefetch extends AbstractPendingMessageCursor implements
        MessageRecoveryListener {

    static private final Log log=LogFactory.getLog(TopicStorePrefetch.class);
   
    private TopicMessageStore store;
    private final LinkedList batchList=new LinkedList();
    private String clientId;
    private String subscriberName;
    private Destination regionDestination;

    boolean empty;
	private MessageId firstMessageId;
	private MessageId lastMessageId;

    /**
     * @param topic
     * @param clientId
     * @param subscriberName
     * @throws IOException
     */
    public TopicStorePrefetch(Topic topic,String clientId,String subscriberName){
        this.regionDestination = topic;
        this.store=(TopicMessageStore)topic.getMessageStore();
        this.clientId=clientId;
        this.subscriberName=subscriberName;
    }

    public void start() throws Exception {
        if(batchList.isEmpty()){
            try{
                fillBatch();
            }catch(Exception e){
                log.error("Failed to fill batch",e);
                throw new RuntimeException(e);
            }
            empty = batchList.isEmpty();
        }    	
    }

    public void stop() throws Exception{
        store.resetBatching(clientId,subscriberName);
        gc();
    }

    /**
     * @return true if there are no pending messages
     */
    public boolean isEmpty(){
        return empty;
    }
    
    public synchronized int size(){
        try{
            return store.getMessageCount(clientId,subscriberName);
        }catch(IOException e){
            log.error(this + " Failed to get the outstanding message count from the store",e);
            throw new RuntimeException(e);
        }
    }
    
    public synchronized void addMessageLast(MessageReference node) throws Exception{
		if(node!=null){
			if( empty ) {
				firstMessageId = node.getMessageId();
				empty=false;
			}
	        lastMessageId = node.getMessageId();
            node.decrementReferenceCount();
        }
    }

    public synchronized boolean hasNext() {
        return !isEmpty();
    }

    public synchronized MessageReference next(){
    	    	
        if( empty ) {
        	return null;
        } else {

        	// We may need to fill in the batch...
            if(batchList.isEmpty()){
                try{
                    fillBatch();
                }catch(Exception e){
                    log.error("Failed to fill batch",e);
                    throw new RuntimeException(e);
                }
                if( batchList.isEmpty()) {
                	return null;
                }
            }

            Message result = (Message)batchList.removeFirst();
        	
        	if( firstMessageId != null ) {
            	// Skip messages until we get to the first message.
        		if( !result.getMessageId().equals(firstMessageId) ) 
        			return null;
        		firstMessageId = null;
        	}
        	if( lastMessageId != null ) {
        		if( result.getMessageId().equals(lastMessageId) ) {
        			empty=true;
        		}
        	}        	
            result.setRegionDestination(regionDestination);
            return result;
        }
    }

    public void reset(){
    }

    // MessageRecoveryListener implementation
    public void finished(){
    }

    public void recoverMessage(Message message) throws Exception{
        message.setRegionDestination(regionDestination);
        message.incrementReferenceCount();
        batchList.addLast(message);
    }

    public void recoverMessageReference(String messageReference)
            throws Exception{
        // shouldn't get called
        throw new RuntimeException("Not supported");
    }

    // implementation
    protected void fillBatch() throws Exception{
        store.recoverNextMessages(clientId,subscriberName,maxBatchSize,this);
    }
    
    public void gc() {
        batchList.clear();
    }
    
    public String toString() {
        return "TopicStorePrefetch" + System.identityHashCode(this) + "("+clientId+","+subscriberName+")";
    }
    
}
