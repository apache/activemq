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
import java.util.Iterator;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.Message;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.kahadaptor.CommandMarshaller;
/**
 * perist pending messages pending message (messages awaiting disptach to a consumer) cursor
 *
 * @version $Revision$
 */
public class FilePendingMessageCursor extends AbstractPendingMessageCursor{
    private ListContainer list;
    private Iterator iter=null;
    private Destination regionDestination;

    /**
     * @param name
     * @param store
     * @throws IOException
     */
    public FilePendingMessageCursor(String name,Store store){
        try{
            list=store.getListContainer(name);
            list.setMarshaller(new CommandMarshaller(new OpenWireFormat()));
            list.setMaximumCacheSize(0);
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

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
        iter=list.listIterator();
    }

    /**
     * add message to await dispatch
     * 
     * @param node
     */
    public void addMessageLast(MessageReference node){
        try{
            regionDestination=node.getMessage().getRegionDestination();
            node.decrementReferenceCount();
        }catch(IOException e){
            throw new RuntimeException(e);
        }
        list.addLast(node);
    }

    /**
     * add message to await dispatch
     * 
     * @param position
     * @param node
     */
    public void addMessageFirst(MessageReference node){
        try{
            regionDestination=node.getMessage().getRegionDestination();
            node.decrementReferenceCount();
        }catch(IOException e){
            throw new RuntimeException(e);
        }
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
        Message message=(Message) iter.next();
        message.setRegionDestination(regionDestination);
        message.incrementReferenceCount();
        return message;
    }

    /**
     * remove the message at the cursor position
     * 
     */
    public void remove(){
        iter.remove();
    }
    
    public void remove(MessageReference node){
        list.remove(node);
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
}
