/*
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
package org.apache.activemq.transport.amqp.client;

import java.io.IOException;
import java.util.Map;

import javax.jms.InvalidClientIDException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.ResourceAllocationException;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.TransactionErrors;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ConnectionError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;

public class AmqpSupport {

    // Symbols used for connection capabilities
    public static final Symbol SOLE_CONNECTION_CAPABILITY = Symbol.valueOf("sole-connection-for-container");
    public static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");
    public static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");

    // Symbols used to announce connection error information
    public static final Symbol CONNECTION_OPEN_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");
    public static final Symbol INVALID_FIELD = Symbol.valueOf("invalid-field");
    public static final Symbol CONTAINER_ID = Symbol.valueOf("container-id");

    // Symbols used to announce connection redirect ErrorCondition 'info'
    public static final Symbol PORT = Symbol.valueOf("port");
    public static final Symbol NETWORK_HOST = Symbol.valueOf("network-host");
    public static final Symbol OPEN_HOSTNAME = Symbol.valueOf("hostname");

    // Symbols used for connection properties
    public static final Symbol QUEUE_PREFIX = Symbol.valueOf("queue-prefix");
    public static final Symbol TOPIC_PREFIX = Symbol.valueOf("topic-prefix");

    public static final Symbol PRODUCT = Symbol.valueOf("product");
    public static final Symbol VERSION = Symbol.valueOf("version");
    public static final Symbol PLATFORM = Symbol.valueOf("platform");

    // Symbols used for receivers.
    public static final Symbol COPY = Symbol.getSymbol("copy");
    public static final Symbol NO_LOCAL_SYMBOL = Symbol.valueOf("no-local");
    public static final Symbol SELECTOR_SYMBOL = Symbol.valueOf("jms-selector");

    // Delivery states
    public static final Rejected REJECTED = new Rejected();
    public static final Modified MODIFIED_FAILED = new Modified();
    public static final Modified MODIFIED_FAILED_UNDELIVERABLE = new Modified();

    // Temporary Destination constants
    public static final Symbol DYNAMIC_NODE_LIFETIME_POLICY = Symbol.valueOf("lifetime-policy");
    public static final String TEMP_QUEUE_CREATOR = "temp-queue-creator:";
    public static final String TEMP_TOPIC_CREATOR = "temp-topic-creator:";

    //----- Static initializer -----------------------------------------------//

    static {
        MODIFIED_FAILED.setDeliveryFailed(true);

        MODIFIED_FAILED_UNDELIVERABLE.setDeliveryFailed(true);
        MODIFIED_FAILED_UNDELIVERABLE.setUndeliverableHere(true);
    }

    //----- Utility Methods --------------------------------------------------//

    /**
     * Given an ErrorCondition instance create a new Exception that best matches
     * the error type.
     *
     * @param errorCondition
     *      The ErrorCondition returned from the remote peer.
     *
     * @return a new Exception instance that best matches the ErrorCondition value.
     */
    public static Exception convertToException(ErrorCondition errorCondition) {
        Exception remoteError = null;

        if (errorCondition != null && errorCondition.getCondition() != null) {
            Symbol error = errorCondition.getCondition();
            String message = extractErrorMessage(errorCondition);

            if (error.equals(AmqpError.UNAUTHORIZED_ACCESS)) {
                remoteError = new JMSSecurityException(message);
            } else if (error.equals(AmqpError.RESOURCE_LIMIT_EXCEEDED)) {
                remoteError = new ResourceAllocationException(message);
            } else if (error.equals(AmqpError.NOT_FOUND)) {
                remoteError = new InvalidDestinationException(message);
            } else if (error.equals(TransactionErrors.TRANSACTION_ROLLBACK)) {
                remoteError = new TransactionRolledBackException(message);
            } else if (error.equals(ConnectionError.REDIRECT)) {
                remoteError = createRedirectException(error, message, errorCondition);
            } else if (error.equals(AmqpError.INVALID_FIELD)) {
                Map<?, ?> info = errorCondition.getInfo();
                if (info != null && CONTAINER_ID.equals(info.get(INVALID_FIELD))) {
                    remoteError = new InvalidClientIDException(message);
                } else {
                    remoteError = new JMSException(message);
                }
            } else {
                remoteError = new JMSException(message);
            }
        } else {
            remoteError = new JMSException("Unknown error from remote peer");
        }

        return remoteError;
    }

    /**
     * Attempt to read and return the embedded error message in the given ErrorCondition
     * object.  If no message can be extracted a generic message is returned.
     *
     * @param errorCondition
     *      The ErrorCondition to extract the error message from.
     *
     * @return an error message extracted from the given ErrorCondition.
     */
    public static String extractErrorMessage(ErrorCondition errorCondition) {
        String message = "Received error from remote peer without description";
        if (errorCondition != null) {
            if (errorCondition.getDescription() != null && !errorCondition.getDescription().isEmpty()) {
                message = errorCondition.getDescription();
            }

            Symbol condition = errorCondition.getCondition();
            if (condition != null) {
                message = message + " [condition = " + condition + "]";
            }
        }

        return message;
    }

    /**
     * When a redirect type exception is received this method is called to create the
     * appropriate redirect exception type containing the error details needed.
     *
     * @param error
     *        the Symbol that defines the redirection error type.
     * @param message
     *        the basic error message that should used or amended for the returned exception.
     * @param condition
     *        the ErrorCondition that describes the redirection.
     *
     * @return an Exception that captures the details of the redirection error.
     */
    public static Exception createRedirectException(Symbol error, String message, ErrorCondition condition) {
        Exception result = null;
        Map<?, ?> info = condition.getInfo();

        if (info == null) {
            result = new IOException(message + " : Redirection information not set.");
        } else {
            String hostname = (String) info.get(OPEN_HOSTNAME);

            String networkHost = (String) info.get(NETWORK_HOST);
            if (networkHost == null || networkHost.isEmpty()) {
                result = new IOException(message + " : Redirection information not set.");
            }

            int port = 0;
            try {
                port = Integer.valueOf(info.get(PORT).toString());
            } catch (Exception ex) {
                result = new IOException(message + " : Redirection information not set.");
            }

            result = new AmqpRedirectedException(message, hostname, networkHost, port);
        }

        return result;
    }
}