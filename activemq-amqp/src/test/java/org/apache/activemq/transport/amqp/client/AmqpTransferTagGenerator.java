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
package org.apache.activemq.transport.amqp.client;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Utility class that can generate and if enabled pool the binary tag values
 * used to identify transfers over an AMQP link.
 */
public final class AmqpTransferTagGenerator {

    public static final int DEFAULT_TAG_POOL_SIZE = 1024;

    private long nextTagId;
    private int maxPoolSize = DEFAULT_TAG_POOL_SIZE;

    private final Set<byte[]> tagPool;

    public AmqpTransferTagGenerator() {
        this(false);
    }

    public AmqpTransferTagGenerator(boolean pool) {
        if (pool) {
            this.tagPool = new LinkedHashSet<byte[]>();
        } else {
            this.tagPool = null;
        }
    }

    /**
     * Retrieves the next available tag.
     *
     * @return a new or unused tag depending on the pool option.
     */
    public byte[] getNextTag() {
        byte[] rc;
        if (tagPool != null && !tagPool.isEmpty()) {
            final Iterator<byte[]> iterator = tagPool.iterator();
            rc = iterator.next();
            iterator.remove();
        } else {
            try {
                rc = Long.toHexString(nextTagId++).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                // This should never happen since we control the input.
                throw new RuntimeException(e);
            }
        }
        return rc;
    }

    /**
     * When used as a pooled cache of tags the unused tags should always be returned once
     * the transfer has been settled.
     *
     * @param data
     *        a previously borrowed tag that is no longer in use.
     */
    public void returnTag(byte[] data) {
        if (tagPool != null && tagPool.size() < maxPoolSize) {
            tagPool.add(data);
        }
    }

    /**
     * Gets the current max pool size value.
     *
     * @return the current max tag pool size.
     */
    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    /**
     * Sets the max tag pool size.  If the size is smaller than the current number
     * of pooled tags the pool will drain over time until it matches the max.
     *
     * @param maxPoolSize
     *        the maximum number of tags to hold in the pool.
     */
    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }
}
