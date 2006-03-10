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
package org.apache.activemq.transport;

import edu.emory.mathcs.backport.java.util.Queue;

import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.transport.replay.ExceptionIfDroppedReplayStrategy;
import org.apache.activemq.transport.replay.ReplayStrategy;

import junit.framework.TestCase;

/**
 * 
 * @version $Revision$
 */
public class ReliableTransportTest extends TestCase {

    protected TransportFilter transport;
    protected StubTransportListener listener = new StubTransportListener();
    protected ReplayStrategy replayStrategy;

    public void testValidSequenceOfPackets() throws Exception {
        int[] sequenceNumbers = { 1, 2, 3, 4, 5, 6, 7 };

        sendStreamOfCommands(sequenceNumbers, true);
    }

    public void testInvalidSequenceOfPackets() throws Exception {
        int[] sequenceNumbers = { 1, 2, /* 3, */  4, 5, 6, 7 };

        sendStreamOfCommands(sequenceNumbers, false);
    }

    protected void sendStreamOfCommands(int[] sequenceNumbers, boolean expected) {
        for (int i = 0; i < sequenceNumbers.length; i++) {
            int commandId = sequenceNumbers[i];
            
            ConsumerInfo info = new ConsumerInfo();
            info.setSelector("Cheese: " + commandId);
            info.setCommandId(commandId);

            transport.onCommand(info);
        }
        
        Queue exceptions = listener.getExceptions();
        Queue commands = listener.getCommands();
        if (expected) {
            if (!exceptions.isEmpty()) {
                Exception e = (Exception) exceptions.remove();
                e.printStackTrace();
                fail("Caught exception: " + e);
            }
            assertEquals("number of messages received", sequenceNumbers.length, commands.size());
        }
        else {
            assertTrue("Should have received an exception!", exceptions.size() > 0);
            Exception e = (Exception) exceptions.remove();
            System.out.println("Caught expected response: " + e);
        }
        
    }

    protected void setUp() throws Exception {
        if (replayStrategy == null) {
            replayStrategy = new ExceptionIfDroppedReplayStrategy();
        }
        transport = new ReliableTransport(new StubTransport(), replayStrategy);
        transport.setTransportListener(listener);
        transport.start();
    }

}
