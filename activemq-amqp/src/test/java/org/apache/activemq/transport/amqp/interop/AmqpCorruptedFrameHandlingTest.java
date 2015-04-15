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
package org.apache.activemq.transport.amqp.interop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.util.Wait;
import org.junit.Test;

/**
 * Test that broker closes connection and allows a new one when the transport
 * receives a bad chunk of data after a successful connect.
 */
public class AmqpCorruptedFrameHandlingTest extends AmqpClientTestSupport {

    @Test(timeout = 60000)
    public void testCanConnect() throws Exception {
        Random random = new Random();
        random.setSeed(System.nanoTime());

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.createConnection();

        connection.setContainerId("ClientID:" + getTestName());
        connection.connect();

        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());

        // Send frame with valid size prefix, but corrupted payload.
        byte[] corruptedFrame = new byte[1024];
        random.nextBytes(corruptedFrame);
        corruptedFrame[0] = 0x0;
        corruptedFrame[1] = 0x0;
        corruptedFrame[2] = 0x4;
        corruptedFrame[3] = 0x0;

        connection.sendRawBytes(corruptedFrame);

        assertTrue("Connection should have dropped.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getCurrentConnectionsCount() == 0;
            }
        }));

        connection.close();
    }
}
