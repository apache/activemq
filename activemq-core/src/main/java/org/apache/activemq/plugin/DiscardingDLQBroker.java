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
 package org.apache.activemq.plugin;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Filip Hanik
 * @version 1.0
 */
public class DiscardingDLQBroker extends BrokerFilter {
    public static Log log = LogFactory.getLog(DiscardingDLQBroker.class);
    private boolean dropTemporaryTopics = true;
    private boolean dropTemporaryQueues = true;
    private boolean dropAll = true;
    private Pattern[] destFilter;
    private int reportInterval = 1000;
    private long dropCount = 0;

    public DiscardingDLQBroker(Broker next) {
        super(next);
    }

    @Override
    public void sendToDeadLetterQueue(ConnectionContext ctx, MessageReference msgRef) {
        if (log.isTraceEnabled()) {
            try {
                log.trace("Discarding DLQ BrokerFilter[pass through] - skipping message:" + (msgRef != null ? msgRef.getMessage() : null));
            } catch (IOException x) {
                log.trace("Discarding DLQ BrokerFilter[pass through] - skipping message:" + msgRef != null ? msgRef : null, x);
            }
        }
        boolean dropped = true;
        Message msg = null;
        ActiveMQDestination dest = null;
        String destName = null;
        try {
            msg = msgRef.getMessage();
            dest = msg.getDestination();
            destName = dest.getPhysicalName();
        }catch (IOException x) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to retrieve message or destination for message going to Dead Letter Queue. message skipped.", x);
            }
        }

        if (dest == null || destName == null ) {
            //do nothing, no need to forward it
            skipMessage("NULL DESTINATION",msgRef);
        } else if (dropAll) {
            //do nothing
            skipMessage("dropAll",msgRef);
        } else if (dropTemporaryTopics && dest.isTemporary() && dest.isTopic()) {
            //do nothing
            skipMessage("dropTemporaryTopics",msgRef);
        } else if (dropTemporaryQueues && dest.isTemporary() && dest.isQueue()) {
            //do nothing
            skipMessage("dropTemporaryQueues",msgRef);
        } else if (destFilter!=null && matches(destName)) {
            //do nothing
            skipMessage("dropOnly",msgRef);
        } else {
            dropped = false;
            next.sendToDeadLetterQueue(ctx, msgRef);
        }
        if (dropped && getReportInterval()>0) {
            if ((++dropCount)%getReportInterval() == 0 ) {
                log.info("Total of "+dropCount+" messages were discarded, since their destination was the dead letter queue");
            }
        }
    }

    public boolean matches(String destName) {
        for (int i=0; destFilter!=null && i<destFilter.length; i++) {
            if (destFilter[i]!=null && destFilter[i].matcher(destName).matches()) {
                return true;
            }
        }
        return false;
    }

    private void skipMessage(String prefix, MessageReference msgRef) {
        if (log.isDebugEnabled()) {
            try {
                String lmsg = "Discarding DLQ BrokerFilter["+prefix+"] - skipping message:" + (msgRef!=null?msgRef.getMessage():null);
                log.debug(lmsg);
            }catch (IOException x) {
                log.debug("Discarding DLQ BrokerFilter["+prefix+"] - skipping message:" + (msgRef!=null?msgRef:null),x);
            }
        }
    }

    public void setDropTemporaryTopics(boolean dropTemporaryTopics) {
        this.dropTemporaryTopics = dropTemporaryTopics;
    }

    public void setDropTemporaryQueues(boolean dropTemporaryQueues) {
        this.dropTemporaryQueues = dropTemporaryQueues;
    }

    public void setDropAll(boolean dropAll) {
        this.dropAll = dropAll;
    }

    public void setDestFilter(Pattern[] destFilter) {
        this.destFilter = destFilter;
    }

    public void setReportInterval(int reportInterval) {
        this.reportInterval = reportInterval;
    }

    public boolean isDropTemporaryTopics() {
        return dropTemporaryTopics;
    }

    public boolean isDropTemporaryQueues() {
        return dropTemporaryQueues;
    }

    public boolean isDropAll() {
        return dropAll;
    }

    public Pattern[] getDestFilter() {
        return destFilter;
    }

    public int getReportInterval() {
        return reportInterval;
    }

}
