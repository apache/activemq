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
package org.apache.activemq.transport.http;

import edu.emory.mathcs.backport.java.util.Queue;
import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

import org.apache.activemq.command.Command;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.util.ServiceStopper;

import java.io.IOException;

/**
 * A server side HTTP based TransportChannel which processes incoming packets
 * and adds outgoing packets onto a {@link Queue} so that they can be dispatched
 * by the HTTP GET requests from the client.
 * 
 * @version $Revision$
 */
public class BlockingQueueTransport extends TransportSupport {
    public static final long MAX_TIMEOUT = 30000L;

    private BlockingQueue queue;

    public BlockingQueueTransport(BlockingQueue channel) {
        this.queue = channel;
    }

    public BlockingQueue getQueue() {
        return queue;
    }

    public void oneway(Command command) throws IOException {
        try {
            boolean success = queue.offer(command, MAX_TIMEOUT, TimeUnit.MILLISECONDS);
            if (!success)
                throw new IOException("Fail to add to BlockingQueue. Add timed out after " + MAX_TIMEOUT + "ms: size=" + queue.size());
        } catch (InterruptedException e) {
            throw new IOException("Fail to add to BlockingQueue. Interrupted while waiting for space: size=" + queue.size());
        }
    }

    protected void doStart() throws Exception {
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
    }   
}
