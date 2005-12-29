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

import org.apache.activemq.command.Command;
import org.apache.activemq.command.Message;
import org.apache.activemq.management.CountStatisticImpl;
import org.apache.activemq.management.StatsImpl;

/**
 * The J2EE Statistics for the Connection.
 * 
 * @version $Revision$
 */
public class ConnectionStatistics extends StatsImpl {

    private CountStatisticImpl enqueues;
    private CountStatisticImpl dequeues;

    public ConnectionStatistics() {

        enqueues = new CountStatisticImpl("enqueues", "The number of messages that have been sent to the connection");
        dequeues = new CountStatisticImpl("dequeues", "The number of messages that have been dispatched from the connection");

        addStatistic("enqueues", enqueues);
        addStatistic("dequeues", dequeues);
    }

    public CountStatisticImpl getEnqueues() {
        return enqueues;
    }

    public CountStatisticImpl getDequeues() {
        return dequeues;
    }

    public void reset() {
        super.reset();
        enqueues.reset();
        dequeues.reset();
    }

    public void setParent(ConnectorStatistics parent) {
        if (parent != null) {
            enqueues.setParent(parent.getEnqueues());
            dequeues.setParent(parent.getDequeues());
        }
        else {
            enqueues.setParent(null);
            dequeues.setParent(null);
        }
    }

    /**
     * Updates the statistics as a command is dispatched into the connection
     */
    public void onCommand(Command command) {
        if (command.isMessageDispatch()) {
            enqueues.increment();
        }
    }

    public void onMessageDequeue(Message message) {
        dequeues.increment();
    }
}