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
package org.apache.activemq.management;

import javax.jms.Destination;

import org.apache.activemq.util.IndentPrinter;

/**
 * Statistics for a JMS producer
 * 
 * @version $Revision: 1.2 $
 */
public class JMSProducerStatsImpl extends JMSEndpointStatsImpl {
    private String destination;

    public JMSProducerStatsImpl(JMSSessionStatsImpl sessionStats, Destination destination) {
        super(sessionStats);
        if (destination != null) {
            this.destination = destination.toString();
        }
    }

    public JMSProducerStatsImpl(CountStatisticImpl messageCount, CountStatisticImpl pendingMessageCount, CountStatisticImpl expiredMessageCount, TimeStatisticImpl messageWaitTime,
                                TimeStatisticImpl messageRateTime, String destination) {
        super(messageCount, pendingMessageCount, expiredMessageCount, messageWaitTime, messageRateTime);
        this.destination = destination;
    }

    public String getDestination() {
        return destination;
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("producer ");
        buffer.append(destination);
        buffer.append(" { ");
        buffer.append(super.toString());
        buffer.append(" }");
        return buffer.toString();
    }

    public void dump(IndentPrinter out) {
        out.printIndent();
        out.print("producer ");
        out.print(destination);
        out.println(" {");
        out.incrementIndent();

        super.dump(out);

        out.decrementIndent();
        out.printIndent();
        out.println("}");
    }
}
