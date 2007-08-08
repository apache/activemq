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

import java.io.IOException;

import javax.jms.JMSException;

/**
 * An exception thrown when the a connection failure is detected (peer might
 * close the connection, or a keep alive times out, etc.)
 * 
 * @version $Revision$
 */
public class ConnectionFailedException extends JMSException {

    private static final long serialVersionUID = 2288453203492073973L;

    public ConnectionFailedException(IOException cause) {
        super("The JMS connection has failed: " + extractMessage(cause));
        initCause(cause);
        setLinkedException(cause);
    }

    public ConnectionFailedException() {
        super("The JMS connection has failed due to a Transport problem");
    }

    static private String extractMessage(IOException cause) {
        String m = cause.getMessage();
        if (m == null || m.length() == 0)
            m = cause.toString();
        return m;
    }

}
