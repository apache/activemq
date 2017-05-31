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

import org.apache.activemq.command.Response;

/**
 * Interface used by the AmqpProtocolConverter for callbacks from the broker.
 */
public interface ResponseHandler {

    /**
     * Called when the Broker has handled a previously issued request and
     * has a response ready.
     *
     * @param converter
     *        the protocol converter that is awaiting the response.
     * @param response
     *        the response from the broker.
     *
     * @throws IOException if an error occurs while processing the response.
     */
    void onResponse(AmqpProtocolConverter converter, Response response) throws IOException;

}
