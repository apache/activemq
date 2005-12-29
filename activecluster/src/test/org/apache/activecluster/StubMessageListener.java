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
package org.apache.activecluster;

import javax.jms.Message;
import javax.jms.MessageListener;
import java.util.ArrayList;
import java.util.List;

/**
 * A mock message listener for testing
 *
 * @version $Revision: 1.2 $
 */
public class StubMessageListener implements MessageListener {
    private List messages = new ArrayList();
    private Object semaphore;

    public StubMessageListener() {
        this(new Object());
    }

    public StubMessageListener(Object semaphore) {
        this.semaphore = semaphore;
    }

    /**
     * @return all the messages on the list so far, clearing the buffer
     */
    public synchronized List flushMessages() {
        List answer = new ArrayList(messages);
        messages.clear();
        return answer;
    }

    public synchronized void onMessage(Message message) {
        messages.add(message);
        synchronized (semaphore) {
            semaphore.notifyAll();
        }
    }

    public void waitForMessageToArrive() {
        System.out.println("Waiting for message to arrive");

        long start = System.currentTimeMillis();

        try {
            if (messages.isEmpty()) {
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
}
