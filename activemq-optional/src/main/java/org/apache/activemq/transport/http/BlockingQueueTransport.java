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
package org.apache.activemq.transport.http;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.util.ServiceStopper;

import java.util.concurrent.CountDownLatch;
/**
 * A server side HTTP based TransportChannel which processes incoming packets
 * and adds outgoing packets onto a {@link Queue} so that they can be dispatched
 * by the HTTP GET requests from the client.
 *
 * @version $Revision$
 */
public class BlockingQueueTransport extends TransportSupport {
	public static CountDownLatch finalizeLatch;
    public static final long MAX_TIMEOUT = 30000L;

    private BlockingQueue<Object> queue;

    public BlockingQueueTransport(BlockingQueue<Object> channel) {
        this.queue = channel;
    }

    @Override
    public void finalize()
    {
    	finalizeLatch.countDown();
    }

    public BlockingQueue<Object> getQueue() {
        return queue;
    }

    public void oneway(Object command) throws IOException {
        try {
            boolean success = queue.offer(command, MAX_TIMEOUT, TimeUnit.MILLISECONDS);
            if (!success) {
                throw new IOException("Fail to add to BlockingQueue. Add timed out after " + MAX_TIMEOUT + "ms: size=" + queue.size());
            }
        } catch (InterruptedException e) {
            throw new IOException("Fail to add to BlockingQueue. Interrupted while waiting for space: size=" + queue.size());
        }
    }


    public String getRemoteAddress() {
        return "blockingQueue_" + queue.hashCode();
    }

    protected void doStart() throws Exception {
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
    }

    public int getReceiveCounter() {
        return 0;
    }
}