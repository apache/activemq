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
package org.apache.activemq.transport.amqp.client.util;

import org.apache.qpid.proton.engine.Receiver;

/**
 * Unmodifiable Receiver wrapper used to prevent test code from accidentally
 * modifying Receiver state.
 */
public class UnmodifiableReceiver extends UnmodifiableLink implements Receiver {

    private final Receiver receiver;

    public UnmodifiableReceiver(Receiver receiver) {
        super(receiver);

        this.receiver = receiver;
    }

    @Override
    public void flow(int credits) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public int recv(byte[] bytes, int offset, int size) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public void drain(int credit) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public boolean draining() {
        return receiver.draining();
    }

    @Override
    public void setDrain(boolean drain) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }
}
