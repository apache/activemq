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
package org.apache.activemq.test.retroactive;

import javax.jms.MessageListener;

import org.apache.activemq.broker.region.policy.MessageQuery;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @version $Revision$
 */
public class DummyMessageQuery implements MessageQuery {
    
    private static final Log LOG = LogFactory.getLog(DummyMessageQuery.class);

    public static final int MESSAGE_COUNT = 10;
    
    public void execute(ActiveMQDestination destination, MessageListener listener) throws Exception {
        LOG.info("Initial query is creating: " + MESSAGE_COUNT + " messages");
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.setText("Initial message: " + i + " loaded from query");
            listener.onMessage(message);
        }
    }

    public boolean validateUpdate(Message message) {
        return true;
    }
}
