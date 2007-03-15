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
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.activemq.broker.region.MessageReference;
/**
 * hold pending messages in a linked list (messages awaiting disptach to a consumer) cursor
 * 
 * @version $Revision$
 */
public class VMPendingMessageCursor extends AbstractPendingMessageCursor{
    private LinkedList<MessageReference> list = new LinkedList<MessageReference>();
    private Iterator<MessageReference> iter = null;
    private MessageReference last;
    
    /**
     * @return true if there are no pending messages
     */
    public boolean isEmpty(){
        return list.isEmpty();
    }

    /**
     * reset the cursor
     *
     */
    public void reset(){
        iter = list.listIterator();
        last=null;
    }
    
    /**
     * add message to await dispatch
     * 
     * @param node
     */
    public void addMessageLast(MessageReference node){
    	node.incrementReferenceCount();
        list.addLast(node);
    }
    
    /**
     * add message to await dispatch
     * @param position 
     * @param node
     */
    public void addMessageFirst(MessageReference node){
    	node.incrementReferenceCount();
        list.addFirst(node);
    }


    /**
     * @return true if there pending messages to dispatch
     */
    public boolean hasNext(){
       return iter.hasNext();
    }

    /**
     * @return the next pending message
     */
    public MessageReference next(){
    	last = (MessageReference) iter.next();
    	return last;
    }

    /**
     * remove the message at the cursor position
     * 
     */
    public void remove(){
    	if( last!=null ) {
    		last.decrementReferenceCount();
    	}
        iter.remove();
    }

    /**
     * @return the number of pending messages
     */
    public int size(){
        return list.size();
    }

    /**
     * clear all pending messages
     * 
     */
    public void clear(){
        list.clear();
    }
    
    public void remove(MessageReference node){
        for(Iterator<MessageReference> i=list.iterator();i.hasNext();){
            MessageReference ref=i.next();
            if(node.getMessageId().equals(ref.getMessageId())){
            	ref.decrementReferenceCount();
                i.remove();
                break;
            }
        }
    }
    
    /**
     * Page in a restricted number of messages
     * @param maxItems
     * @return a list of paged in messages
     */
    public LinkedList<MessageReference> pageInList(int maxItems) {
        return list;
    }
}
