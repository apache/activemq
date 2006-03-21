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
package org.apache.activemq.store.kahadaptor;

import java.io.IOException;
import java.util.Iterator;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
/**
 * An implementation of {@link org.apache.activemq.store.MessageStore} which uses a JPS Container
 * 
 * @version $Revision: 1.7 $
 */
public class KahaMessageStore implements MessageStore{
    protected final ActiveMQDestination destination;
    protected final MapContainer messageContainer;

    public KahaMessageStore(MapContainer container,ActiveMQDestination destination) throws IOException{
        this.messageContainer=container;
        this.destination=destination;
    }

    public void addMessage(ConnectionContext context,Message message) throws IOException{
        messageContainer.put(message.getMessageId().toString(),message);
    }

    public void addMessageReference(ConnectionContext context,MessageId messageId,long expirationTime,String messageRef)
                    throws IOException{
        messageContainer.put(messageId.toString(),messageRef);
    }

    public Message getMessage(MessageId identity) throws IOException{
        return (Message) messageContainer.get(identity.toString());
    }

    public String getMessageReference(MessageId identity) throws IOException{
        return (String) messageContainer.get(identity.toString());
    }

    public void removeMessage(ConnectionContext context,MessageAck ack) throws IOException{
        messageContainer.remove(ack.getLastMessageId().toString());
    }

    public void removeMessage(MessageId msgId) throws IOException{
        messageContainer.remove(msgId.toString());
    }

    public void recover(MessageRecoveryListener listener) throws Exception{
        for(Iterator iter=messageContainer.values().iterator();iter.hasNext();){
            Object msg=(Object) iter.next();
            if(msg.getClass()==String.class){
                listener.recoverMessageReference((String) msg);
            }else{
                listener.recoverMessage((Message) msg);
            }
        }
        listener.finished();
    }

    public void start() throws IOException{}

    public void stop(long timeout) throws IOException{}

    public void removeAllMessages(ConnectionContext context) throws IOException{
        messageContainer.clear();
    }

    public ActiveMQDestination getDestination(){
        return destination;
    }

    public void delete(){
        messageContainer.clear();
    }
}
