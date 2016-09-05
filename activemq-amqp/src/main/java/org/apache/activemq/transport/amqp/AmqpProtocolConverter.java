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

import java.io.IOException;

import org.apache.activemq.command.Command;

/**
 * Interface that defines the API for any AMQP protocol converter ised to
 * map AMQP mechanics to ActiveMQ and back.
 */
public interface AmqpProtocolConverter {

    /**
     * A new incoming data packet from the remote peer is handed off to the
     * protocol converter for processing.  The type can vary and be either an
     * AmqpHeader at the handshake phase or a byte buffer containing the next
     * incoming frame data from the remote.
     *
     * @param data
     *        the next incoming data object from the remote peer.
     *
     * @throws Exception if an error occurs processing the incoming data packet.
     */
    void onAMQPData(Object data) throws Exception;

    /**
     * Called when the transport detects an exception that the converter
     * needs to respond to.
     *
     * @param error
     *        the error that triggered this call.
     */
    void onAMQPException(IOException error);

    /**
     * Incoming Command object from ActiveMQ.
     *
     * @param command
     *        the next incoming command from the broker.
     *
     * @throws Exception if an error occurs processing the command.
     */
    void onActiveMQCommand(Command command) throws Exception;

    /**
     * On changes to the transport tracing options the Protocol Converter
     * should update its internal state so that the proper AMQP data is
     * logged.
     */
    void updateTracer();

    /**
     * Perform any keep alive processing for the connection such as sending
     * empty frames or closing connections due to remote end being inactive
     * for to long.
     *
     * @returns the amount of milliseconds to wait before performing another check.
     *
     * @throws IOException if an error occurs on writing heart-beats to the wire.
     */
    long keepAlive() throws IOException;

}
