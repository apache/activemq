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
package org.apache.activemq.broker.region.policy;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;

/**
 * This is the default Topic recovery policy which does not recover any messages.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class NoSubscriptionRecoveryPolicy implements SubscriptionRecoveryPolicy {

    public boolean add(ConnectionContext context, MessageReference node) throws Throwable {
        return true;
    }

    public void recover(ConnectionContext context, Topic topic, Subscription sub) throws Throwable {
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public Message[] browse(ActiveMQDestination dest) throws Throwable{
        return new Message[0];
    }

}
