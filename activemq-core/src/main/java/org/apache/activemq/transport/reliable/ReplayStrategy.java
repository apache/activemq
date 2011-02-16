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
 * A pluggable strategy for how to deal with dropped packets.
 * 
 * 
 */
public interface ReplayStrategy {

    /**
     * Deals with a dropped packet. 
     * 
     * @param transport the transport on which the packet was dropped
     * @param expectedCounter the expected command counter
     * @param actualCounter the actual command counter
     * @param nextAvailableCounter TODO
     * @return true if the command should be buffered or false if it should be discarded
     */
    boolean onDroppedPackets(ReliableTransport transport, int expectedCounter, int actualCounter, int nextAvailableCounter) throws IOException;

    void onReceivedPacket(ReliableTransport transport, long expectedCounter);

}

