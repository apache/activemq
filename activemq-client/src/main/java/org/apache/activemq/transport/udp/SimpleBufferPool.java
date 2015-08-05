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

/**
 * A simple implementation of {@link ByteBufferPool} which does no pooling and just
 * creates new buffers each time
 */
public class SimpleBufferPool implements ByteBufferPool {

    private int defaultSize;
    private boolean useDirect;

    public SimpleBufferPool() {
        this(false);
    }

    public SimpleBufferPool(boolean useDirect) {
        this.useDirect = useDirect;
    }

    @Override
    public synchronized ByteBuffer borrowBuffer() {
        return createBuffer();
    }

    @Override
    public void returnBuffer(ByteBuffer buffer) {
    }

    @Override
    public void setDefaultSize(int defaultSize) {
        this.defaultSize = defaultSize;
    }

    public boolean isUseDirect() {
        return useDirect;
    }

    /**
     * Sets whether direct buffers are used or not
     */
    public void setUseDirect(boolean useDirect) {
        this.useDirect = useDirect;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }

    protected ByteBuffer createBuffer() {
        if (useDirect) {
            return ByteBuffer.allocateDirect(defaultSize);
        } else {
            return ByteBuffer.allocate(defaultSize);
        }
    }
}
