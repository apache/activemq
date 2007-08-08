/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.region;

import java.io.IOException;
import javax.jms.InvalidSelectorException;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.filter.MessageEvaluationContext;

public class QueueBrowserSubscription extends QueueSubscription {

    boolean browseDone;

    public QueueBrowserSubscription(Broker broker, ConnectionContext context, ConsumerInfo info)
        throws InvalidSelectorException {
        super(broker, context, info);
    }

    protected boolean canDispatch(MessageReference node) {
        return !((QueueMessageReference)node).isAcked();
    }

    public synchronized String toString() {
        return "QueueBrowserSubscription:" + " consumer=" + info.getConsumerId() + ", destinations="
               + destinations.size() + ", dispatched=" + dispatched.size() + ", delivered="
               + this.prefetchExtension + ", pending=" + getPendingQueueSize();
    }

    public void browseDone() throws Exception {
        browseDone = true;
        add(QueueMessageReference.NULL_MESSAGE);
    }

    public boolean matches(MessageReference node, MessageEvaluationContext context) throws IOException {
        return !browseDone && super.matches(node, context);
    }

    /**
     * Since we are a browser we don't really remove the message from the queue.
     */
    protected void acknowledge(ConnectionContext context, final MessageAck ack, final MessageReference n)
        throws IOException {
    }

}
