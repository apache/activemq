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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;

/**
 * Interface used to define the operations needed to implement an AMQP
 * Link based endpoint, i.e. Sender, Receiver or Coordinator.
 */
public interface AmqpLink extends AmqpResource {

    /**
     * Close the Link with an error indicating the reson for the close.
     *
     * @param error
     *        the error that prompted the close.
     */
    void close(ErrorCondition error);

    /**
     * Request from the remote peer to detach this resource.
     */
    void detach();

    /**
     * Handles an incoming flow control.
     *
     * @throws Excption if an error occurs during the flow processing.
     */
    void flow() throws Exception;

    /**
     * Called when a new Delivery arrives for the given Link.
     *
     * @param delivery
     *        the newly arrived delivery on this link.
     *
     * @throws Exception if an error occurs while processing the new Delivery.
     */
    void delivery(Delivery delivery) throws Exception;

    /**
     * Handle work necessary on commit of transacted resources associated with
     * this Link instance.
     *
     * @param txnId
     *      The Transaction ID being committed.
     *
     * @throws Exception if an error occurs while performing the commit.
     */
    void commit(LocalTransactionId txnId) throws Exception;

    /**
     * Handle work necessary on rollback of transacted resources associated with
     * this Link instance.
     *
     * @param txnId
     *      The Transaction ID being rolled back.
     *
     * @throws Exception if an error occurs while performing the rollback.
     */
    void rollback(LocalTransactionId txnId) throws Exception;

    /**
     * @return the ActiveMQDestination that this link is servicing.
     */
    public ActiveMQDestination getDestination();

    /**
     * Sets the ActiveMQDestination that this link will be servicing.
     *
     * @param destination
     *        the ActiveMQDestination that this link services.
     */
    public void setDestination(ActiveMQDestination destination);

    /**
     * Adds a new Runnable that is called on close of this link.
     *
     * @param action
     *        a Runnable that will be executed when the link closes or detaches.
     */
    public void addCloseAction(Runnable action);

}
