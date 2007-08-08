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
package org.apache.activemq.transport.udp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A default implementation of {@link BufferPool} which keeps a pool of direct
 * byte buffers.
 * 
 * @version $Revision$
 */
public class DefaultBufferPool extends SimpleBufferPool implements ByteBufferPool {

    private List buffers = new ArrayList();
    private Object lock = new Object();

    public DefaultBufferPool() {
        super(true);
    }

    public DefaultBufferPool(boolean useDirect) {
        super(useDirect);
    }

    public synchronized ByteBuffer borrowBuffer() {
        synchronized (lock) {
            int size = buffers.size();
            if (size > 0) {
                return (ByteBuffer) buffers.remove(size - 1);
            }
        }
        return createBuffer();
    }

    public void returnBuffer(ByteBuffer buffer) {
        synchronized (lock) {
            buffers.add(buffer);
        }
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
        synchronized (lock) {
            /*
             * for (Iterator iter = buffers.iterator(); iter.hasNext();) {
             * ByteBuffer buffer = (ByteBuffer) iter.next(); }
             */
            buffers.clear();
        }
    }

}
