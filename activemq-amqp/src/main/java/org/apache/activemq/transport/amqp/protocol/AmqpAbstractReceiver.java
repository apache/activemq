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
package org.apache.activemq.transport.amqp.protocol;

import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base that provides common services for AMQP Receiver types.
 */
public abstract class AmqpAbstractReceiver extends AmqpAbstractLink<Receiver> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAbstractReceiver.class);

    protected ByteArrayOutputStream current = new ByteArrayOutputStream();
    protected final byte[] recvBuffer = new byte[1024 * 8];

    /**
     * Handle create of new AMQP Receiver instance.
     *
     * @param session
     *        the AmqpSession that servers as the parent of this Link.
     * @param endpoint
     *        the Receiver endpoint being managed by this class.
     */
    public AmqpAbstractReceiver(AmqpSession session, Receiver endpoint) {
        super(session, endpoint);
    }

    @Override
    public void detach() {
    }

    @Override
    public void flow() throws Exception {
    }

    /**
     * Provide the receiver endpoint with the given amount of credits.
     *
     * @param credits
     *        the credit value to pass on to the wrapped Receiver.
     */
    public void flow(int credits) {
        getEndpoint().flow(credits);
    }

    @Override
    public void commit() throws Exception {
    }

    @Override
    public void rollback() throws Exception {
    }

    @Override
    public void delivery(Delivery delivery) throws Exception {

        if (!delivery.isReadable()) {
            LOG.debug("Delivery was not readable!");
            return;
        }

        if (current == null) {
            current = new ByteArrayOutputStream();
        }

        int count;
        while ((count = getEndpoint().recv(recvBuffer, 0, recvBuffer.length)) > 0) {
            current.write(recvBuffer, 0, count);
        }

        // Expecting more deliveries..
        if (count == 0) {
            return;
        }

        try {
            processDelivery(delivery, current.toBuffer());
        } finally {
            getEndpoint().advance();
            current = null;
        }
    }

    protected abstract void processDelivery(Delivery delivery, Buffer deliveryBytes) throws Exception;

}
