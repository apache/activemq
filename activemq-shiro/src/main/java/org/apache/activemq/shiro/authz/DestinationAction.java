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
package org.apache.activemq.shiro.authz;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * A {@code DestinationAction} represents behavior being taken on a particular {@link ActiveMQDestination}, such as
 * creation, removal, and reading messages from it or writing messages to it.  The exact behavior being taken on the
 * specific {@link #getDestination() destination} is represented as a {@link #getVerb() verb} property, which is one of
 * the following string tokens:
 * <table>
 * <tr>
 * <th>Verb</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>{@code create}</td>
 * <td>Create a specific destination.</td>
 * </tr>
 * <tr>
 * <td>{@code remove}</td>
 * <td>Remove a specific destination.</td>
 * </tr>
 * <tr>
 * <td>{@code read}</td>
 * <td>Read (consume) messages from a specific destination.</td>
 * </tr>
 * <tr>
 * <td>{@code write}</td>
 * <td>Write messages to a specific destination.</td>
 * </tr>
 * </table>
 *
 * @since 5.10.0
 */
public class DestinationAction implements Action {

    private final ConnectionContext connectionContext;
    private final ActiveMQDestination destination;
    private final String verb;

    public DestinationAction(ConnectionContext connectionContext, ActiveMQDestination destination, String verb) {
        if (connectionContext == null) {
            throw new IllegalArgumentException("ConnectionContext argument cannot be null.");
        }
        if (destination == null) {
            throw new IllegalArgumentException("ActiveMQDestination argument cannot be null.");
        }
        if (verb == null) {
            throw new IllegalArgumentException("verb argument cannot be null.");
        }

        this.connectionContext = connectionContext;
        this.destination = destination;
        this.verb = verb;
    }

    public ConnectionContext getConnectionContext() {
        return connectionContext;
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }

    public String getVerb() {
        return verb;
    }

    @Override
    public String toString() {
        return this.verb + " destination: " + destination;
    }
}
