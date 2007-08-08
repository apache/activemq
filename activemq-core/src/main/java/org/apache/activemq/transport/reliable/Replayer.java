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
 * Used by a {@link ReplayBuffer} to replay buffers back over an unreliable transport
 * 
 * @version $Revision$
 */
public interface Replayer {

    /**
     * Sends the given buffer back to the transport
     * if the buffer could be found - otherwise maybe send some kind
     * of exception
     * 
     * @param commandId the command ID
     * @param buffer the buffer to be sent - or null if the buffer no longer exists in the buffer
     */
    void sendBuffer(int commandId, Object buffer) throws IOException;
}
