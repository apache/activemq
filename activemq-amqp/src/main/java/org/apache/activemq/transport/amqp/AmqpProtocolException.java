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

public class AmqpProtocolException extends IOException {

    private static final long serialVersionUID = -2869735532997332242L;

    private final String symbolicName;
    private final boolean fatal;

    public AmqpProtocolException() {
        this(null);
    }

    public AmqpProtocolException(String s) {
        this(s, false);
    }

    public AmqpProtocolException(String s, boolean fatal) {
        this(s, fatal, null);
    }

    public AmqpProtocolException(String s, String msg) {
        this(s, msg, false, null);
    }

    public AmqpProtocolException(String s, boolean fatal, Throwable cause) {
        this("error", s, fatal, cause);
    }

    public AmqpProtocolException(String symbolicName, String s, boolean fatal, Throwable cause) {
        super(s);
        this.symbolicName = symbolicName;
        this.fatal = fatal;
        initCause(cause);
    }

    public boolean isFatal() {
        return fatal;
    }

    public String getSymbolicName() {
        return symbolicName;
    }
}
