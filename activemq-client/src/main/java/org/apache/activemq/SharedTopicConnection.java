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
package org.apache.activemq;

import jakarta.jms.JMSException;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IdGenerator;

/**
 * Connection extension that creates {@link SharedTopicSession} instances
 * instead of plain {@link ActiveMQSession}, enabling JMS 3.1 shared
 * topic subscription support.
 */
public class SharedTopicConnection extends ActiveMQConnection {

    protected SharedTopicConnection(Transport transport, IdGenerator clientIdGenerator,
            IdGenerator connectionIdGenerator, JMSStatsImpl factoryStats) throws Exception {
        super(transport, clientIdGenerator, connectionIdGenerator, factoryStats);
    }

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        ensureConnectionInfoSent();
        if (!transacted) {
            if (acknowledgeMode == Session.SESSION_TRANSACTED) {
                throw new JMSException(
                        "acknowledgeMode SESSION_TRANSACTED cannot be used for an non-transacted Session");
            } else if (acknowledgeMode < Session.SESSION_TRANSACTED
                    || acknowledgeMode > ActiveMQSession.MAX_ACK_CONSTANT) {
                throw new JMSException("invalid acknowledgeMode: " + acknowledgeMode
                        + ". Valid values are Session.AUTO_ACKNOWLEDGE (1), "
                        + "Session.CLIENT_ACKNOWLEDGE (2), Session.DUPS_OK_ACKNOWLEDGE (3), "
                        + "ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE (4) or for transacted sessions "
                        + "Session.SESSION_TRANSACTED (0)");
            }
        }
        return new SharedTopicSession(this, getNextSessionId(),
                transacted ? Session.SESSION_TRANSACTED : acknowledgeMode,
                isDispatchAsync(), isAlwaysSessionAsync());
    }
}
