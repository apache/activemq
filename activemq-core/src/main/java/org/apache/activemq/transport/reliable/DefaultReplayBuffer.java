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
package org.apache.activemq.transport.reliable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @version $Revision$
 */
public class DefaultReplayBuffer implements ReplayBuffer {

    private final int size;
    private ReplayBufferListener listener;
    private Map map;
    private int lowestCommandId = 1;
    private Object lock = new Object();

    public DefaultReplayBuffer(int size) {
        this.size = size;
        map = createMap(size);
    }

    public void addBuffer(int commandId, Object buffer) {
        synchronized (lock) {
            int max = size - 1;
            while (map.size() >= max) {
                // lets find things to evict
                Object evictedBuffer = map.remove(new Integer(++lowestCommandId));
                onEvictedBuffer(lowestCommandId, evictedBuffer);
            }
            map.put(new Integer(commandId), buffer);
        }
    }

    public void setReplayBufferListener(ReplayBufferListener bufferPoolAdapter) {
        this.listener = bufferPoolAdapter;
    }

    public void replayMessages(int fromCommandId, int toCommandId, Replayer replayer) throws IOException {
        for (int i = fromCommandId; i <= toCommandId; i++) {
            Object buffer = null;
            synchronized (lock) {
                buffer = map.get(new Integer(i));
            }
            replayer.sendBuffer(i, buffer);
        }
    }

    protected Map createMap(int maximumSize) {
        return new HashMap(maximumSize);
    }

    protected void onEvictedBuffer(int commandId, Object buffer) {
        if (listener != null) {
            listener.onBufferDiscarded(commandId, buffer);
        }
    }

}
