/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activemq.broker.region;

import java.io.IOException;

import org.activemq.command.ConsumerId;
import org.activemq.command.Message;
import org.activemq.command.MessageId;

/**
 * Keeps track of a message that is flowing through the Broker.  This 
 * object may hold a hard reference to the message or only hold the
 * id of the message if the message has been persisted on in a MessageStore.
 * 
 * @version $Revision: 1.15 $
 */
public interface MessageReference {
    
    public MessageId getMessageId();
    public Message getMessageHardRef();
    public Message getMessage() throws IOException;
    public boolean isPersistent();
    
    public Destination getRegionDestination();
    
    public int getRedeliveryCounter();
    public void incrementRedeliveryCounter();
    
    public int getReferenceCount();
    
    public int incrementReferenceCount();
    public int decrementReferenceCount();
    public ConsumerId getTargetConsumerId();
    
}
