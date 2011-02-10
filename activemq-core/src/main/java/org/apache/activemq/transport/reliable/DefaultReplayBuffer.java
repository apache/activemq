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
package org.apache.activemq.transport.reliable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @version $Revision$
 */
public class DefaultReplayBuffer implements ReplayBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultReplayBuffer.class);

    private final int size;
    private ReplayBufferListener listener;
    private Map<Integer, Object> map;
    private int lowestCommandId = 1;
    private Object lock = new Object();

    public DefaultReplayBuffer(int size) {
        this.size = size;
        map = createMap(size);
    }

    public void addBuffer(int commandId, Object buffer) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding command ID: " + commandId + " to replay buffer: " + this + " object: " + buffer);
        }
        synchronized (lock) {
            int max = size - 1;
            while (map.size() >= max) {
                // lets find things to evict
                Object evictedBuffer = map.remove(Integer.valueOf(++lowestCommandId));
                onEvictedBuffer(lowestCommandId, evictedBuffer);
            }
            map.put(Integer.valueOf(commandId), buffer);
        }
    }

    public void setReplayBufferListener(ReplayBufferListener bufferPoolAdapter) {
        this.listener = bufferPoolAdapter;
    }

    public void replayMessages(int fromCommandId, int toCommandId, Replayer replayer) throws IOException {
        if (replayer == null) {
            throw new IllegalArgumentException("No Replayer parameter specified");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Buffer: " + this + " replaying messages from: " + fromCommandId + " to: " + toCommandId);
        }
        for (int i = fromCommandId; i <= toCommandId; i++) {
            Object buffer = null;
            synchronized (lock) {
                buffer = map.get(Integer.valueOf(i));
            }
            replayer.sendBuffer(i, buffer);
        }
    }

    protected Map<Integer, Object> createMap(int maximumSize) {
        return new HashMap<Integer, Object>(maximumSize);
    }

    protected void onEvictedBuffer(int commandId, Object buffer) {
        if (listener != null) {
            listener.onBufferDiscarded(commandId, buffer);
        }
    }
}
