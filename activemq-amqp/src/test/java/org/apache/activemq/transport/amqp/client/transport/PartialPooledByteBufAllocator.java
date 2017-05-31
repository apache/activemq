/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.amqp.client.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

/**
 * A {@link ByteBufAllocator} which is partial pooled. Which means only direct
 * {@link ByteBuf}s are pooled. The rest is unpooled.
 *
 * @author <a href="mailto:nmaurer@redhat.com">Norman Maurer</a>
 */
public class PartialPooledByteBufAllocator implements ByteBufAllocator {

    private static final ByteBufAllocator POOLED = new PooledByteBufAllocator(false);
    private static final ByteBufAllocator UNPOOLED = new UnpooledByteBufAllocator(false);

    public static final PartialPooledByteBufAllocator INSTANCE = new PartialPooledByteBufAllocator();

    private PartialPooledByteBufAllocator() {
    }

    @Override
    public ByteBuf buffer() {
        return UNPOOLED.heapBuffer();
    }

    @Override
    public ByteBuf buffer(int initialCapacity) {
        return UNPOOLED.heapBuffer(initialCapacity);
    }

    @Override
    public ByteBuf buffer(int initialCapacity, int maxCapacity) {
        return UNPOOLED.heapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf ioBuffer() {
        return UNPOOLED.heapBuffer();
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity) {
        return UNPOOLED.heapBuffer(initialCapacity);
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
        return UNPOOLED.heapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf heapBuffer() {
        return UNPOOLED.heapBuffer();
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity) {
        return UNPOOLED.heapBuffer(initialCapacity);
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        return UNPOOLED.heapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf directBuffer() {
        return POOLED.directBuffer();
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity) {
        return POOLED.directBuffer(initialCapacity);
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
        return POOLED.directBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        return UNPOOLED.compositeHeapBuffer();
    }

    @Override
    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        return UNPOOLED.compositeHeapBuffer(maxNumComponents);
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        return UNPOOLED.compositeHeapBuffer();
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        return UNPOOLED.compositeHeapBuffer(maxNumComponents);
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer() {
        return POOLED.compositeDirectBuffer();
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
        return POOLED.compositeDirectBuffer();
    }

    @Override
    public boolean isDirectBufferPooled() {
        return true;
    }

    @Override
    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
        return POOLED.calculateNewCapacity(minNewCapacity, maxCapacity);
    }
}
