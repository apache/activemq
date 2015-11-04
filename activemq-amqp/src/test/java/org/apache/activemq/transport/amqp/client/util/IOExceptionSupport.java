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
package org.apache.activemq.transport.amqp.client.util;

import java.io.IOException;

/**
 * Used to make throwing IOException instances easier.
 */
public class IOExceptionSupport {

    /**
     * Checks the given cause to determine if it's already an IOException type and
     * if not creates a new IOException to wrap it.
     *
     * @param cause
     *        The initiating exception that should be cast or wrapped.
     *
     * @return an IOException instance.
     */
    public static IOException create(Throwable cause) {
        if (cause instanceof IOException) {
            return (IOException) cause;
        }

        String message = cause.getMessage();
        if (message == null || message.length() == 0) {
            message = cause.toString();
        }

        return new IOException(message, cause);
    }
}
