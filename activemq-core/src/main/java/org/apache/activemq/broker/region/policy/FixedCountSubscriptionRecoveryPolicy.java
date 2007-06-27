/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.ArrayList;
import java.util.List;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.SubscriptionRecovery;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.filter.MessageEvaluationContext;
/**
 * This implementation of {@link SubscriptionRecoveryPolicy} will keep a fixed count 
 * of last messages.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class FixedCountSubscriptionRecoveryPolicy implements SubscriptionRecoveryPolicy{
    volatile private MessageReference messages[];
    private int maximumSize=100;
    private int tail=0;
    
    public SubscriptionRecoveryPolicy copy() {
        FixedCountSubscriptionRecoveryPolicy rc = new  FixedCountSubscriptionRecoveryPolicy();
        rc.setMaximumSize(maximumSize);
        return rc;
    }

    synchronized public boolean add(ConnectionContext context,MessageReference node) throws Exception{
        messages[tail++]=node;
        if(tail>=messages.length)
            tail=0;
        return true;
    }

    synchronized public void recover(ConnectionContext context,Topic topic,SubscriptionRecovery sub) throws Exception{
        // Re-dispatch the last message seen.
        int t=tail;
        // The buffer may not have rolled over yet..., start from the front
        if(messages[t]==null)
            t=0;
        // Well the buffer is really empty then.
        if(messages[t]==null)
            return;
        // Keep dispatching until t hit's tail again.
        do{
            MessageReference node=messages[t];
            sub.addRecoveredMessage(context,node);
            t++;
            if(t>=messages.length)
                t=0;
        }while(t!=tail);
    }

    public void start() throws Exception{
        messages=new MessageReference[maximumSize];
    }

    public void stop() throws Exception{
        messages=null;
    }

    public int getMaximumSize(){
        return maximumSize;
    }

    /**
     * Sets the maximum number of messages that this destination will hold around in RAM
     */
    public void setMaximumSize(int maximumSize){
        this.maximumSize=maximumSize;
    }

    public synchronized Message[] browse(ActiveMQDestination destination) throws Exception{
        List result=new ArrayList();
        DestinationFilter filter=DestinationFilter.parseFilter(destination);
        int t=tail;
        if(messages[t]==null)
            t=0;
        if(messages[t]!=null){
            do{
                MessageReference ref=messages[t];
                Message message=ref.getMessage();
                if(filter.matches(message.getDestination())){
                    result.add(message);
                }
                t++;
                if(t>=messages.length)
                    t=0;
            }while(t!=tail);
        }
        return (Message[]) result.toArray(new Message[result.size()]);
    }

}
