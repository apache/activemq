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
package org.apache.activemq.broker;

import java.io.IOException;

import org.apache.activemq.Service;
import org.apache.activemq.broker.region.ConnectionStatistics;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.Response;

/**
 *
 */
public interface Connection extends Service {

    /**
     * @return the connector that created this connection.
     */
    Connector getConnector();

    /**
     * Sends a message to the client.
     *
     * @param message the message to send to the client.
     */
    void dispatchSync(Command message);

    /**
     * Sends a message to the client.
     *
     * @param command
     */
    void dispatchAsync(Command command);

    /**
     * Services a client command and submits it to the broker.
     *
     * @param command
     * @return Response
     */
    Response service(Command command);

    /**
     * Handles an unexpected error associated with a connection.
     *
     * @param error
     */
    void serviceException(Throwable error);

    /**
     * @return true if the Connection is slow
     */
    boolean isSlow();

    /**
     * @return if after being marked, the Connection is still writing
     */
    boolean isBlocked();

    /**
     * @return true if the Connection is connected
     */
    boolean isConnected();

    /**
     * @return true if the Connection is active
     */
    boolean isActive();

    /**
     * Returns the number of messages to be dispatched to this connection
     */
    int getDispatchQueueSize();

    /**
     * Returns the statistics for this connection
     */
    ConnectionStatistics getStatistics();

    /**
     * @return true if the Connection will process control commands
     */
    boolean isManageable();

    /**
     * @return the source address for this connection
     */
    String getRemoteAddress();

    void serviceExceptionAsync(IOException e);

    String getConnectionId();

    /**
     * return true if a network connection
     *
     * @return if this is a network connection
     */
    boolean isNetworkConnection();

    /**
     * @return true if a fault tolerant connection
     */
    boolean isFaultTolerantConnection();

    void updateClient(ConnectionControl control);


    /**
     * Returns the number of active transactions established on this Connection.
     *
     * @return the number of active transactions established on this Connection..
     */
    public int getActiveTransactionCount();

    /**
     * Returns the number of active transactions established on this Connection.
     *
     * @return the number of active transactions established on this Connection..
     */
    public Long getOldestActiveTransactionDuration();

}
