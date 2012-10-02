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
package org.apache.activemq.transport.stomp;

import java.io.IOException;

/**
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class ProtocolException extends IOException {

    private static final long serialVersionUID = -2869735532997332242L;

    private final boolean fatal;

    public ProtocolException() {
        this(null);
    }

    public ProtocolException(String s) {
        this(s, false);
    }

    public ProtocolException(String s, boolean fatal) {
        this(s, fatal, null);
    }

    public ProtocolException(String s, boolean fatal, Throwable cause) {
        super(s);
        this.fatal = fatal;
        initCause(cause);
    }

    public boolean isFatal() {
        return fatal;
    }

}
