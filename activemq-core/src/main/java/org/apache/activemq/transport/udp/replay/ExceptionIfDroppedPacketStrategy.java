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
package org.apache.activemq.transport.udp.replay;

import java.io.IOException;

/**
 * Throws an exception if packets are dropped causing the transport to be closed.
 * 
 * @version $Revision$
 */
public class ExceptionIfDroppedPacketStrategy implements DatagramReplayStrategy {

    public void onDroppedPackets(long expectedCounter, long actualCounter) throws IOException {
        long count = actualCounter - expectedCounter;
        throw new IOException("" + count +  " packet(s) dropped. Expected: " + expectedCounter + " but was: " + actualCounter);
    }

    public void onReceivedPacket(long expectedCounter) {
    }

}
