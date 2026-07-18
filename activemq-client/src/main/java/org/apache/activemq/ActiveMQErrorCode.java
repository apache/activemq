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
package org.apache.activemq;

/**
 * Error code constants carried on {@code ExceptionResponse.errorCode}
 * and surfaced as {@link jakarta.jms.JMSException#getErrorCode()}.
 *
 * <p>Codes follow the pattern {@code AMQ-NNNNN} where the numeric range
 * groups by protocol layer, bottom-up:
 * <ul>
 *   <li>{@code 10xxx} — transport (TCP/SSL connection, socket errors)</li>
 *   <li>{@code 20xxx} — wire format (OpenWire framing, marshalling, version negotiation)</li>
 *   <li>{@code 30xxx} — session (session state, transaction, acknowledge mode)</li>
 *   <li>{@code 40xxx} — validation (destination, selector, subscription name)</li>
 *   <li>{@code 50xxx} — consumer/producer/subscription (shared subscription lifecycle, type conflicts, dispatch)</li>
 * </ul>
 */
public final class ActiveMQErrorCode {

    private ActiveMQErrorCode() {}

    // --- Transport (10xxx) ---

    // reserved

    // --- Wire format (20xxx) ---

    /** Message frame exceeds the negotiated maximum frame size. */
    public static final String MAX_FRAME_SIZE_EXCEEDED = "AMQ-20001";

    // --- Session (30xxx) ---

    // reserved

    // --- Validation (40xxx) ---

    /** Topic destination is null. */
    public static final String INVALID_DESTINATION = "AMQ-40001";

    /** Subscription name is null or empty. */
    public static final String INVALID_SUBSCRIPTION_NAME = "AMQ-40003";

    // --- Consumer / producer / subscription (50xxx) ---

    /** Consumer attempted to join a shared subscription with a different selector. */
    public static final String SELECTOR_MISMATCH = "AMQ-50001";

    /** A shared and unshared durable subscription have the same name and client identifier. */
    public static final String SUBSCRIPTION_TYPE_CONFLICT = "AMQ-50002";

    /** Unsubscribe attempted while the shared durable subscription has active consumers. */
    public static final String SUBSCRIPTION_IN_USE = "AMQ-50003";

    /** A shared durable subscription with the same name is already active. */
    public static final String SUBSCRIPTION_ALREADY_EXISTS = "AMQ-50004";
}
