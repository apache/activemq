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
package org.apache.activemq.command;

import org.apache.activemq.state.CommandVisitor;

/**
 * 
 * @openwire:marshaller code="6"
 * @version $Revision: 1.13 $
 */
public class ProducerInfo extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.PRODUCER_INFO;

    protected ProducerId producerId;
    protected ActiveMQDestination destination;
    protected BrokerId[] brokerPath;
    protected boolean dispatchAsync;
    protected int windowSize;

    public ProducerInfo() {
    }

    public ProducerInfo(ProducerId producerId) {
        this.producerId = producerId;
    }

    public ProducerInfo(SessionInfo sessionInfo, long producerId) {
        this.producerId = new ProducerId(sessionInfo.getSessionId(), producerId);
    }

    public ProducerInfo copy() {
        ProducerInfo info = new ProducerInfo();
        copy(info);
        return info;
    }

    public void copy(ProducerInfo info) {
        super.copy(info);
        info.producerId = producerId;
        info.destination = destination;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ProducerId getProducerId() {
        return producerId;
    }

    public void setProducerId(ProducerId producerId) {
        this.producerId = producerId;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    public RemoveInfo createRemoveCommand() {
        RemoveInfo command = new RemoveInfo(getProducerId());
        command.setResponseRequired(isResponseRequired());
        return command;
    }

    /**
     * The route of brokers the command has moved through.
     * 
     * @openwire:property version=1 cache=true
     */
    public BrokerId[] getBrokerPath() {
        return brokerPath;
    }

    public void setBrokerPath(BrokerId[] brokerPath) {
        this.brokerPath = brokerPath;
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processAddProducer(this);
    }

    /**
     * If the broker should dispatch messages from this producer async. Since
     * sync dispatch could potentally block the producer thread, this could be
     * an important setting for the producer.
     * 
     * @openwire:property version=2
     */
    public boolean isDispatchAsync() {
        return dispatchAsync;
    }

    public void setDispatchAsync(boolean dispatchAsync) {
        this.dispatchAsync = dispatchAsync;
    }

    /**
     * Used to configure the producer window size. A producer will send up to
     * the configured window size worth of payload data to the broker before
     * waiting for an Ack that allows him to send more.
     * 
     * @openwire:property version=3
     */
    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

}
