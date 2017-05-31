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
package org.apache.activemq.transport.amqp;

import org.apache.activemq.transport.amqp.message.InboundTransformer;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

/**
 * Creates the default AMQP WireFormat object used to configure the protocol support.
 */
public class AmqpWireFormatFactory implements WireFormatFactory {

    private long maxFrameSize = AmqpWireFormat.DEFAULT_MAX_FRAME_SIZE;
    private int maxAmqpFrameSize = AmqpWireFormat.DEFAULT_ANQP_FRAME_SIZE;
    private int idelTimeout = AmqpWireFormat.DEFAULT_IDLE_TIMEOUT;
    private int producerCredit = AmqpWireFormat.DEFAULT_PRODUCER_CREDIT;
    private String transformer = InboundTransformer.TRANSFORMER_NATIVE;
    private boolean allowNonSaslConnections = AmqpWireFormat.DEFAULT_ALLOW_NON_SASL_CONNECTIONS;

    @Override
    public WireFormat createWireFormat() {
        AmqpWireFormat wireFormat = new AmqpWireFormat();

        wireFormat.setMaxFrameSize(getMaxFrameSize());
        wireFormat.setMaxAmqpFrameSize(getMaxAmqpFrameSize());
        wireFormat.setIdleTimeout(getIdelTimeout());
        wireFormat.setProducerCredit(getProducerCredit());
        wireFormat.setTransformer(getTransformer());
        wireFormat.setAllowNonSaslConnections(isAllowNonSaslConnections());

        return wireFormat;
    }

    public int getMaxAmqpFrameSize() {
        return maxAmqpFrameSize;
    }

    public void setMaxAmqpFrameSize(int maxAmqpFrameSize) {
        this.maxAmqpFrameSize = maxAmqpFrameSize;
    }

    public long getMaxFrameSize() {
        return maxFrameSize;
    }

    public void setMaxFrameSize(long maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public int getIdelTimeout() {
        return idelTimeout;
    }

    public void setIdelTimeout(int idelTimeout) {
        this.idelTimeout = idelTimeout;
    }

    public int getProducerCredit() {
        return producerCredit;
    }

    public void setProducerCredit(int producerCredit) {
        this.producerCredit = producerCredit;
    }

    public String getTransformer() {
        return transformer;
    }

    public void setTransformer(String transformer) {
        this.transformer = transformer;
    }

    public boolean isAllowNonSaslConnections() {
        return allowNonSaslConnections;
    }

    public void setAllowNonSaslConnections(boolean allowNonSaslConnections) {
        this.allowNonSaslConnections = allowNonSaslConnections;
    }
}
