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
package org.apache.activemq.usecases;

import javax.jms.Destination;
import javax.jms.Message;

import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.test.JmsTopicSendReceiveWithTwoConnectionsTest;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class CompositeConsumeTest extends JmsTopicSendReceiveWithTwoConnectionsTest {

    public void testSendReceive() throws Exception {
        messages.clear();

        Destination[] destinations = getDestinations();
        int destIdx = 0;

        for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);

            if (verbose) {
                log.info("About to send a message: " + message + " with text: " + data[i]);
            }

            producer.send(destinations[destIdx], message);

            if (++destIdx >= destinations.length) {
                destIdx = 0;
            }
        }

        assertMessagesAreReceived();
    }

    /**
     * Returns the subscription subject
     */
    protected String getSubject() {
        return getPrefix() + "FOO.BAR," + getPrefix() + "FOO.X.Y," + getPrefix() + "BAR.>";
    }

    /**
     * Returns the destinations on which we publish
     */
    protected Destination[] getDestinations() {
        return new Destination[]{new ActiveMQTopic(getPrefix() + "FOO.BAR"), new ActiveMQTopic(getPrefix() + "BAR.WHATNOT.XYZ"), new ActiveMQTopic(getPrefix() + "FOO.X.Y")};
    }

    protected String getPrefix() {
        return super.getSubject() + ".";
    }
}
