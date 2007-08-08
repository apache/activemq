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
    
    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        enqueues.setEnabled(enabled);
        dequeues.setEnabled(enabled);
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


}
