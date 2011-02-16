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

import org.slf4j.Logger;

/**
 * Interface for classes that will be called by the TransportLogger
 * class to actually write to a log file.
 * Every class that implements this interface has do be declared in
 * the resources/META-INF/services/org/apache/activemq/transport/logwriters
 * directory, by creating a file with the name of the writer (for example
 * "default") and including the line
 * class=org.apache.activemq.transport.logwriters.(Name of the LogWriter class)
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com
 * 
 */
public interface LogWriter {

    /**
     * Writes a header message to the log.
     * @param log The log to be written to.
     */
    public void initialMessage(Logger log);
    
    /**
     * Writes a message to a log when a request command is sent.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logRequest (Logger log, Object command);
    
    /**
     * Writes a message to a log when a response command is received.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logResponse (Logger log, Object response);

    /**
     * Writes a message to a log when an asynchronous equest command is sent.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logAsyncRequest (Logger log, Object command);
    
    /**
     * Writes a message to a log when message is sent.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logOneWay (Logger log, Object command);
    
    /**
     * Writes a message to a log when message is received.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logReceivedCommand (Logger log, Object command);
    
    /**
     * Writes a message to a log when an exception is received.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logReceivedException (Logger log, IOException error);
    
}
