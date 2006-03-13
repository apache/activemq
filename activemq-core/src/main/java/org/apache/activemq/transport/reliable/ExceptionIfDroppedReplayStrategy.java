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
package org.apache.activemq.transport.reliable;

import java.io.IOException;

/**
 * Throws an exception if packets are dropped causing the transport to be
 * closed.
 * 
 * @version $Revision$
 */
public class ExceptionIfDroppedReplayStrategy implements ReplayStrategy {

    private int maximumDifference = 5;

    public boolean onDroppedPackets(ReliableTransport transport, int expectedCounter, int actualCounter) throws IOException {
        int difference = actualCounter - expectedCounter;
        long count = Math.abs(difference);
        if (count > maximumDifference) {
            throw new IOException("Packets dropped on: " + transport + " count: " + count + " expected: " + expectedCounter + " but was: " + actualCounter);
        }
        
        // lets discard old commands
        return difference > 0;
    }

    public void onReceivedPacket(ReliableTransport transport, long expectedCounter) {
    }

    public int getMaximumDifference() {
        return maximumDifference;
    }

    /**
     * Sets the maximum allowed difference between an expected packet and an
     * actual packet before an error occurs
     */
    public void setMaximumDifference(int maximumDifference) {
        this.maximumDifference = maximumDifference;
    }

}
