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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.util.IndentPrinter;

/**
 * Statistics for a number of JMS connections
 * 
 * @version $Revision: 1.2 $
 */
public class JMSStatsImpl extends StatsImpl {
    private List connections = new CopyOnWriteArrayList();

    public JMSStatsImpl() {
    }

    public JMSConnectionStatsImpl[] getConnections() {
        Object[] connectionArray = connections.toArray();
        int size = connectionArray.length;
        JMSConnectionStatsImpl[] answer = new JMSConnectionStatsImpl[size];
        for (int i = 0; i < size; i++) {
            ActiveMQConnection connection = (ActiveMQConnection)connectionArray[i];
            answer[i] = connection.getConnectionStats();
        }
        return answer;
    }

    public void addConnection(ActiveMQConnection connection) {
        connections.add(connection);
    }

    public void removeConnection(ActiveMQConnection connection) {
        connections.remove(connection);
    }

    public void dump(IndentPrinter out) {
        out.printIndent();
        out.println("factory {");
        out.incrementIndent();
        JMSConnectionStatsImpl[] array = getConnections();
        for (int i = 0; i < array.length; i++) {
            JMSConnectionStatsImpl connectionStat = (JMSConnectionStatsImpl)array[i];
            connectionStat.dump(out);
        }
        out.decrementIndent();
        out.printIndent();
        out.println("}");
        out.flush();
    }

    /**
     * @param enabled the enabled to set
     */
    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        JMSConnectionStatsImpl[] stats = getConnections();
        for (int i = 0, size = stats.length; i < size; i++) {
            stats[i].setEnabled(enabled);
        }

    }
}
