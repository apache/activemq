/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq.systest.impl;

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.Message;
import javax.jms.MessageListener;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple consumer which is useful for testing which can be used to wait until
 * the consumer has received a specific number of messages.
 * 
 * @author Mike Perham
 * @version $Revision$
 */
public class AgentMessageListener implements MessageListener {
    private List messages = new CopyOnWriteArrayList();
    private Object semaphore = new Object();;

    public  void stop() {
        messages.clear();
    }

    /**
     * @return all the messages on the list so far, clearing the buffer
     */
    public List flushMessages() {
        List answer = new ArrayList(messages);
        messages.clear();
        return answer;
    }

    public void onMessage(Message message) {
        System.out.println("Received message: " + message);

            messages.add(message);

        synchronized (semaphore) {
            semaphore.notifyAll();
        }
    }

    public void waitForMessageToArrive() {
        System.out.println("Waiting for message to arrive");

        long start = System.currentTimeMillis();

        try {
            if (hasReceivedMessage()) {
                synchronized (semaphore) {
                    semaphore.wait(4000);
                }
            }
        }
        catch (InterruptedException e) {
            System.out.println("Caught: " + e);
        }
        long end = System.currentTimeMillis() - start;

        System.out.println("End of wait for " + end + " millis");
    }

    public void waitForMessagesToArrive(int messageCount) {
        System.out.println("Waiting for message to arrive");

        long start = System.currentTimeMillis();

        for (int i = 0; i < 10; i++) {
            try {
                if (hasReceivedMessages(messageCount)) {
                    break;
                }
                synchronized (semaphore) {
                    semaphore.wait(1000);
                }
            }
            catch (InterruptedException e) {
                System.out.println("Caught: " + e);
            }
        }
        long end = System.currentTimeMillis() - start;

        System.out.println("End of wait for " + end + " millis");
    }

    protected boolean hasReceivedMessage() {
        return messages.isEmpty();
    }

    protected boolean hasReceivedMessages(int messageCount) {
        return messages.size() >= messageCount;
    }

}
