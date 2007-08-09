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

import org.apache.activemq.Service;

/**
 * Represents a pool of {@link ByteBuffer} instances. 
 * This strategy could just create new buffers for each call or
 * it could pool them.
 * 
 * @version $Revision$
 */
public interface ByteBufferPool extends Service {

    /**
     * Extract a buffer from the pool.
     */
    ByteBuffer borrowBuffer();

    /**
     * Returns the buffer to the pool or just discards it for a non-pool strategy
     */
    void returnBuffer(ByteBuffer buffer);

    /**
     * Sets the default size of the buffers
     */
    void setDefaultSize(int defaultSize);

}
