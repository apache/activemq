/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
 *
 **/
package org.activemq.broker.region.policy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.activemq.broker.ConnectionContext;
import org.activemq.broker.region.MessageReference;
import org.activemq.broker.region.Subscription;
import org.activemq.broker.region.Topic;
import org.activemq.filter.MessageEvaluationContext;
import org.activemq.thread.Scheduler;

/**
 * This implementation of {@link SubscriptionRecoveryPolicy} will keep a timed
 * buffer of messages around in memory and use that to recover new
 * subscriptions.
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision$
 */
public class TimedSubscriptionRecoveryPolicy implements SubscriptionRecoveryPolicy {

    private static final int GC_INTERVAL = 1000;

    // TODO: need to get a better synchronized linked list that has little
    // contention between enqueuing and dequeuing
    private final List buffer = Collections.synchronizedList(new LinkedList());
    private volatile long lastGCRun = System.currentTimeMillis();

    private long recoverDuration = 60 * 1000; // Buffer for 1 min.

    static class TimestampWrapper {
        public MessageReference message;
        public long timestamp;

        public TimestampWrapper(MessageReference message, long timestamp) {
            this.message = message;
            this.timestamp = timestamp;
        }
    }
    
    private final Runnable gcTask = new Runnable() {
        public void run() {
            gc();
        }
    };

    public boolean add(ConnectionContext context, MessageReference message) throws Throwable {
        buffer.add(new TimestampWrapper(message, lastGCRun));
        return true;
    }

    public void recover(ConnectionContext context, Topic topic, Subscription sub) throws Throwable {
        
        // Re-dispatch the messages from the buffer.
        ArrayList copy = new ArrayList(buffer);

        if (!copy.isEmpty()) {
            MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
            try {
                for (Iterator iter = copy.iterator(); iter.hasNext();) {
                    TimestampWrapper timestampWrapper = (TimestampWrapper) iter.next();
                    MessageReference message = timestampWrapper.message;
                    msgContext.setDestination(message.getRegionDestination().getActiveMQDestination());
                    msgContext.setMessageReference(message);
                    if (sub.matches(message, msgContext)) {
                        sub.add(timestampWrapper.message);
                    }
                }
            }finally {
                msgContext.clear();
            }
        }
    }

    public void start() throws Exception {
        Scheduler.executePeriodically(gcTask, GC_INTERVAL);
    }

    public void stop() throws Exception {
        Scheduler.cancel(gcTask);
    }

    public void gc() {
        lastGCRun = System.currentTimeMillis();
        while (buffer.size() > 0) {
            TimestampWrapper timestampWrapper = (TimestampWrapper) buffer.get(0);
            if( lastGCRun > timestampWrapper.timestamp+recoverDuration ) {
                // GC it.
                buffer.remove(0);
            }
            else {
                break;
            }
        }
    }

    public long getRecoverDuration() {
        return recoverDuration;
    }

    public void setRecoverDuration(long recoverDuration) {
        this.recoverDuration = recoverDuration;
    }

}
