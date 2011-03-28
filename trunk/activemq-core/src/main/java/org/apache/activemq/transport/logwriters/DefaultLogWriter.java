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
package org.apache.activemq.transport.logwriters;

import java.io.IOException;

import org.apache.activemq.transport.LogWriter;
import org.slf4j.Logger;

/**
 * Implementation of LogWriter interface to keep ActiveMQ's
 * old logging format.
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com
 * 
 */
public class DefaultLogWriter implements LogWriter {

    // doc comment inherited from LogWriter
    public void initialMessage(Logger log) {
        // Default log writer does nothing here
    }

    // doc comment inherited from LogWriter
    public void logRequest (Logger log, Object command) {
        log.debug("SENDING REQUEST: "+command);
    }

    // doc comment inherited from LogWriter
    public void logResponse (Logger log, Object response) {
        log.debug("GOT RESPONSE: "+response);
    }

    // doc comment inherited from LogWriter
    public void logAsyncRequest (Logger log, Object command) {
        log.debug("SENDING ASNYC REQUEST: "+command);
    }

    // doc comment inherited from LogWriter
    public void logOneWay (Logger log, Object command) {
        log.debug("SENDING: "+command);
    }

    // doc comment inherited from LogWriter
    public void logReceivedCommand (Logger log, Object command) {
        log.debug("RECEIVED: " + command);
    }

    // doc comment inherited from LogWriter
    public void logReceivedException (Logger log, IOException error) {
        log.debug("RECEIVED Exception: "+error, error);
    }


}
