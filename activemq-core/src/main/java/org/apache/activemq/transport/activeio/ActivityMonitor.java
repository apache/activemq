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
package org.apache.activemq.transport.activeio;

import java.io.IOException;

import org.activeio.AsyncChannel;
import org.activeio.FilterAsyncChannel;
import org.activeio.Packet;
import org.apache.activemq.management.CountStatisticImpl;

/**
 * Used to make sure that commands are arriving periodically from the peer of the transport.  
 * 
 * @version $Revision$
 */
public class ActivityMonitor extends FilterAsyncChannel {

    final CountStatisticImpl writeCounter = new CountStatisticImpl("writeCounter", "The number of bytes written to the transport");
    final  CountStatisticImpl readCounter = new CountStatisticImpl("readCoutner", "The number bytes written to the transport");
    
    public ActivityMonitor(AsyncChannel next) {
        super(next);
    }
    
    public void onPacket(Packet packet) {
        readCounter.add(packet.remaining());
        super.onPacket(packet);
    }
    
    public void write(Packet packet) throws IOException {
        writeCounter.add(packet.remaining());
        super.write(packet);
    }

    public CountStatisticImpl getWriteCounter() {
        return writeCounter;
    }
    
    public CountStatisticImpl getReadCounter() {
        return readCounter;
    }
    
}
