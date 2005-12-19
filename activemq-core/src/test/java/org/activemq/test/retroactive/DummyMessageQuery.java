/**
 * 
 * Copyright 2005 LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq.test.retroactive;

import org.activemq.broker.region.policy.MessageQuery;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ActiveMQTextMessage;
import org.activemq.command.Message;

import javax.jms.MessageListener;

/**
 *
 * @version $Revision$
 */
public class DummyMessageQuery implements MessageQuery {

    public static int messageCount = 10;
    
    public void execute(ActiveMQDestination destination, MessageListener listener) throws Exception {
        System.out.println("Initial query is creating: " + messageCount + " messages");
        for (int i = 0; i < messageCount; i++) {
            ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.setText("Initial message: " + i + " loaded from query");
            listener.onMessage(message);
        }
    }

    public boolean validateUpdate(Message message) {
        return true;
    }
}
