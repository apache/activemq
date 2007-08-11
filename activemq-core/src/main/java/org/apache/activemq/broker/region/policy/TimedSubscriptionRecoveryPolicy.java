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
package org.apache.activemq.broker.region.policy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.SubscriptionRecovery;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.thread.Scheduler;

/**
 * This implementation of {@link SubscriptionRecoveryPolicy} will keep a timed
 * buffer of messages around in memory and use that to recover new
 * subscriptions.
 * 
 * @org.apache.xbean.XBean
 * @version $Revision$
 */
public class TimedSubscriptionRecoveryPolicy implements SubscriptionRecoveryPolicy {

    private static final int GC_INTERVAL = 1000;

    // TODO: need to get a better synchronized linked list that has little
    // contention between enqueuing and dequeuing
    private final List<TimestampWrapper> buffer = Collections.synchronizedList(new LinkedList<TimestampWrapper>());
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

    public SubscriptionRecoveryPolicy copy() {
        TimedSubscriptionRecoveryPolicy rc = new TimedSubscriptionRecoveryPolicy();
        rc.setRecoverDuration(recoverDuration);
        return rc;
    }

    public boolean add(ConnectionContext context, MessageReference message) throws Exception {
        buffer.add(new TimestampWrapper(message, lastGCRun));
        return true;
    }

    public void recover(ConnectionContext context, Topic topic, SubscriptionRecovery sub) throws Exception {
        // Re-dispatch the messages from the buffer.
        ArrayList<TimestampWrapper> copy = new ArrayList<TimestampWrapper>(buffer);
        if (!copy.isEmpty()) {
            for (Iterator<TimestampWrapper> iter = copy.iterator(); iter.hasNext();) {
                TimestampWrapper timestampWrapper = iter.next();
                MessageReference message = timestampWrapper.message;
                sub.addRecoveredMessage(context, message);
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
            TimestampWrapper timestampWrapper = buffer.get(0);
            if (lastGCRun > timestampWrapper.timestamp + recoverDuration) {
                // GC it.
                buffer.remove(0);
            } else {
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

    public Message[] browse(ActiveMQDestination destination) throws Exception {
        List<Message> result = new ArrayList<Message>();
        ArrayList<TimestampWrapper> copy = new ArrayList<TimestampWrapper>(buffer);
        DestinationFilter filter = DestinationFilter.parseFilter(destination);
        for (Iterator<TimestampWrapper> iter = copy.iterator(); iter.hasNext();) {
            TimestampWrapper timestampWrapper = iter.next();
            MessageReference ref = timestampWrapper.message;
            Message message = ref.getMessage();
            if (filter.matches(message.getDestination())) {
                result.add(message);
            }
        }
        return result.toArray(new Message[result.size()]);
    }

}
