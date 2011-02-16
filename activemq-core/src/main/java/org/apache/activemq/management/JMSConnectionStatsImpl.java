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

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.util.IndentPrinter;

/**
 * Statistics for a JMS connection
 * 
 * 
 */
public class JMSConnectionStatsImpl extends StatsImpl {
    private List sessions;
    private boolean transactional;

    public JMSConnectionStatsImpl(List sessions, boolean transactional) {
        this.sessions = sessions;
        this.transactional = transactional;
    }

    public JMSSessionStatsImpl[] getSessions() {
        // lets make a snapshot before we process them
        Object[] sessionArray = sessions.toArray();
        int size = sessionArray.length;
        JMSSessionStatsImpl[] answer = new JMSSessionStatsImpl[size];
        for (int i = 0; i < size; i++) {
            ActiveMQSession session = (ActiveMQSession)sessionArray[i];
            answer[i] = session.getSessionStats();
        }
        return answer;
    }

    public void reset() {
        super.reset();
        JMSSessionStatsImpl[] stats = getSessions();
        int size = stats.length;
        for (int i = 0; i < size; i++) {
            stats[i].reset();
        }
    }

    /**
     * @param enabled the enabled to set
     */
    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        JMSSessionStatsImpl[] stats = getSessions();
        int size = stats.length;
        for (int i = 0; i < size; i++) {
            stats[i].setEnabled(enabled);
        }

    }

    public boolean isTransactional() {
        return transactional;
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer("connection{ ");
        JMSSessionStatsImpl[] array = getSessions();
        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            buffer.append(Integer.toString(i));
            buffer.append(" = ");
            buffer.append(array[i]);
        }
        buffer.append(" }");
        return buffer.toString();
    }

    public void dump(IndentPrinter out) {
        out.printIndent();
        out.println("connection {");
        out.incrementIndent();
        JMSSessionStatsImpl[] array = getSessions();
        for (int i = 0; i < array.length; i++) {
            JMSSessionStatsImpl sessionStat = (JMSSessionStatsImpl)array[i];
            out.printIndent();
            out.println("session {");
            out.incrementIndent();
            sessionStat.dump(out);
            out.decrementIndent();
            out.printIndent();
            out.println("}");
        }
        out.decrementIndent();
        out.printIndent();
        out.println("}");
        out.flush();
    }
}
