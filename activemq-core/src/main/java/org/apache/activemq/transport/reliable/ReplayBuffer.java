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

/**
 * This class keeps around a buffer of old commands which have been sent on
 * an unreliable transport. The buffers are of type Object as they could be datagrams
 * or byte[] or ByteBuffer - depending on the underlying transport implementation.
 * 
 * @version $Revision$
 */
public interface ReplayBuffer {

    /**
     * Submit a buffer for caching around for a period of time, during which time it can be replayed
     * to users interested in it.
     */
    public void addBuffer(int commandId, Object buffer);
    
    public void setReplayBufferListener(ReplayBufferListener bufferPoolAdapter);
    
    public void replayMessages(int fromCommandId, int toCommandId, Replayer replayer) throws IOException;
}
