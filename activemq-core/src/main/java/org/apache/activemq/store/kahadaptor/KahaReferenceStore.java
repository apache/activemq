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

package org.apache.activemq.store.kahadaptor;

import java.io.IOException;
import java.util.Set;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.ReferenceStore;

public class KahaReferenceStore implements ReferenceStore{

    protected final ActiveMQDestination destination;
    protected final MapContainer<MessageId,ReferenceRecord> messageContainer;
    protected KahaReferenceStoreAdapter adapter;
    protected StoreEntry batchEntry=null;

    public KahaReferenceStore(KahaReferenceStoreAdapter adapter,MapContainer container,ActiveMQDestination destination) throws IOException{
        this.adapter = adapter;
        this.messageContainer=container;
        this.destination=destination;
    }

    public void start(){
    }

    public void stop(){
    }

    protected MessageId getMessageId(Object object){
        return new MessageId(((ReferenceRecord)object).messageId);
    }

    public synchronized void addMessage(ConnectionContext context,Message message) throws IOException{
        throw new RuntimeException("Use addMessageReference instead");
    }

    public synchronized Message getMessage(MessageId identity) throws IOException{
        throw new RuntimeException("Use addMessageReference instead");
    }

    protected void recover(MessageRecoveryListener listener,Object msg) throws Exception{
        ReferenceRecord record=(ReferenceRecord)msg;
        listener.recoverMessageReference(new MessageId(record.messageId));
    }

    public synchronized void recover(MessageRecoveryListener listener) throws Exception{
        for(StoreEntry entry=messageContainer.getFirst();entry!=null;entry=messageContainer.getNext(entry)){
            ReferenceRecord record=messageContainer.getValue(entry);
            recover(listener,new MessageId(record.messageId));
        }
        listener.finished();
    }

    public synchronized void recoverNextMessages(int maxReturned,MessageRecoveryListener listener) throws Exception{
        StoreEntry entry=batchEntry;
        if(entry==null){
            entry=messageContainer.getFirst();
        }else{
            entry=messageContainer.refresh(entry);
            entry=messageContainer.getNext(entry);
            if (entry==null) {
                batchEntry=null;
            }
        }
        if(entry!=null){
            int count=0;
            do{
                Object msg=messageContainer.getValue(entry);
                if(msg!=null){
                    recover(listener,msg);
                    count++;
                }
                batchEntry=entry;
                entry=messageContainer.getNext(entry);
            }while(entry!=null&&count<maxReturned&&listener.hasSpace());
        }
        listener.finished();
    }

    public void addMessageReference(ConnectionContext context,MessageId messageId,ReferenceData data)
            throws IOException{
        ReferenceRecord record=new ReferenceRecord(messageId.toString(),data);
        messageContainer.put(messageId,record);
        addInterest(record);
    }

    public ReferenceData getMessageReference(MessageId identity) throws IOException{
        ReferenceRecord result=messageContainer.get(identity);
        if(result==null)
            return null;
        return result.data;
    }

    public void addReferenceFileIdsInUse(){
        for(StoreEntry entry=messageContainer.getFirst();entry!=null;entry=messageContainer.getNext(entry)){
            ReferenceRecord msg=(ReferenceRecord)messageContainer.getValue(entry);
            addInterest(msg);
        }
    }

    public void removeMessage(ConnectionContext context,MessageAck ack) throws IOException{
        removeMessage(ack.getLastMessageId());
    }

    public synchronized void removeMessage(MessageId msgId) throws IOException{
        ReferenceRecord rr = messageContainer.remove(msgId);
        removeInterest(rr);
        if(messageContainer.isEmpty()){
            resetBatching();
        }
    }

    public synchronized void removeAllMessages(ConnectionContext context) throws IOException{
        messageContainer.clear();
    }

    public ActiveMQDestination getDestination(){
        return destination;
    }

    public synchronized void delete(){
        messageContainer.clear();
    }

    public void resetBatching(){
        batchEntry=null;
    }

    public int getMessageCount(){
        return messageContainer.size();
    }

    public void setUsageManager(UsageManager usageManager){
    }

    public boolean isSupportForCursors(){
        return true;
    }

    /**
     * @param startAfter
     * @see org.apache.activemq.store.ReferenceStore#setBatch(org.apache.activemq.command.MessageId)
     */
    public void setBatch(MessageId startAfter){
        resetBatching();
        if (startAfter != null) {
            batchEntry = messageContainer.getEntry(startAfter);
        }
        
    }

    public boolean supportsExternalBatchControl(){
        return true;
    }
    
    void removeInterest(ReferenceRecord rr) {
        adapter.removeInterestInRecordFile(rr.data.getFileId());
    }
    
    void addInterest(ReferenceRecord rr) {
        adapter.addInterestInRecordFile(rr.data.getFileId());
    }
}
