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

package org.apache.activemq.store.rapid;

import org.apache.activeio.journal.RecordLocation;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;

public class RapidMessageReference {
    public final MessageId messageId;
    public final long expiration;
    public final RecordLocation location;
    
    public RapidMessageReference(Message message, RecordLocation location) {
        this.messageId = message.getMessageId();
        this.expiration = message.getExpiration();
        this.location=location;
    }

    public long getExpiration() {
        return expiration;
    }

    public MessageId getMessageId() {
        return messageId;
    }
    
    public RecordLocation getLocation() {
        return location;
    }
}