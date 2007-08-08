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
package org.apache.activemq.transport;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision$
 */
public class TransportLogger extends TransportFilter {

    private static int lastId;
    private final Log log;

    public TransportLogger(Transport next) {
        this(next, LogFactory.getLog(TransportLogger.class.getName() + ".Connection:" + getNextId()));
    }

    synchronized private static int getNextId() {
        return ++lastId;
    }

    public TransportLogger(Transport next, Log log) {
        super(next);
        this.log = log;
    }

    public Object request(Object command) throws IOException {
        log.debug("SENDING REQUEST: " + command);
        Object rc = super.request(command);
        log.debug("GOT RESPONSE: " + rc);
        return rc;
    }

    public Object request(Object command, int timeout) throws IOException {
        log.debug("SENDING REQUEST: " + command);
        Object rc = super.request(command, timeout);
        log.debug("GOT RESPONSE: " + rc);
        return rc;
    }

    public FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException {
        log.debug("SENDING ASNYC REQUEST: " + command);
        FutureResponse rc = next.asyncRequest(command, responseCallback);
        return rc;
    }

    public void oneway(Object command) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("SENDING: " + command);
        }
        next.oneway(command);
    }

    public void onCommand(Object command) {
        if (log.isDebugEnabled()) {
            log.debug("RECEIVED: " + command);
        }
        getTransportListener().onCommand(command);
    }

    public void onException(IOException error) {
        if (log.isDebugEnabled()) {
            log.debug("RECEIVED Exception: " + error, error);
        }
        getTransportListener().onException(error);
    }

    public String toString() {
        return next.toString();
    }
}
