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
package org.apache.activemq.broker.region;

import javax.jms.InvalidSelectorException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.filter.MessageEvaluationContext;

public class QueueBrowserSubscription extends PrefetchSubscription {
        
    boolean browseDone;
    
    public QueueBrowserSubscription(ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {
        super(context, info);
    }
        
    protected boolean canDispatch(MessageReference node) {
        return !((IndirectMessageReference)node).isAcked();
    }
    
    public String toString() {
        return 
            "QueueBrowserSubscription:" +
            " consumer="+info.getConsumerId()+
            ", destinations="+destinations.size()+
            ", dispatched="+dispatched.size()+
            ", delivered="+this.delivered+
            ", matched="+this.matched.size();
    }

    public void browseDone() throws Throwable {
        browseDone = true;
        add(IndirectMessageReference.END_OF_BROWSE_MARKER);
    }
    
    protected MessageDispatch createMessageDispatch(MessageReference node, Message message) {
        if( node == IndirectMessageReference.END_OF_BROWSE_MARKER ) {
            MessageDispatch md = new MessageDispatch();
            md.setMessage(null);
            md.setConsumerId( info.getConsumerId() );
            md.setDestination( null );
            return md;
        } else {
            return super.createMessageDispatch(node, message);
        }
    }
    public boolean matches(MessageReference node, MessageEvaluationContext context) {
        return !browseDone && super.matches(node, context);
    }
}
