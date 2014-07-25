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
 * This TransportFilter implementation writes output to a log
 * as it intercepts commands / events before sending them to the
 * following layer in the Transport stack.
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com
 * 
 */
public class TransportLogger extends TransportFilter {

    private final Logger log;
    private boolean logging;
    private final LogWriter logWriter;
    private TransportLoggerView view;

    public TransportLogger(Transport next, Logger log, boolean startLogging, LogWriter logWriter) {
        // Changed constructor to pass the implementation of the LogWriter interface
        // that will be used to write the messages.
        super(next);
        this.log = log;
        this.logging = startLogging;
        this.logWriter = logWriter;
    }

    /**
     * Returns true if logging is activated for this TransportLogger, false otherwise.
     * @return true if logging is activated for this TransportLogger, false otherwise.
     */
    public boolean isLogging() {
        return logging;
    }

    /**
     * Sets if logging should be activated for this TransportLogger.
     * @param logging true to activate logging, false to deactivate.
     */
    public void setLogging(boolean logging) {
        this.logging = logging;
    } 

    public Object request(Object command) throws IOException {
        // Changed this method to use a LogWriter object to actually 
        // print the messages to the log, and only in case of logging 
        // being active, instead of logging the message directly.
        if (logging)
            logWriter.logRequest(log, command);
        Object rc = super.request(command);
        if (logging)
            logWriter.logResponse(log, command);
        return rc;
    }

    public Object request(Object command, int timeout) throws IOException {
        // Changed this method to use a LogWriter object to actually 
        // print the messages to the log, and only in case of logging 
        // being active, instead of logging the message directly.
        if (logging)
            logWriter.logRequest(log, command);
        Object rc = super.request(command, timeout);
        if (logging)
            logWriter.logResponse(log, command);
        return rc;
    }

    public FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException {
        // Changed this method to use a LogWriter object to actually 
        // print the messages to the log, and only in case of logging 
        // being active, instead of logging the message directly.
        if (logging)
            logWriter.logAsyncRequest(log, command);
        FutureResponse rc = next.asyncRequest(command, responseCallback);
        return rc;
    }

    public void oneway(Object command) throws IOException {
        // Changed this method to use a LogWriter object to actually 
        // print the messages to the log, and only in case of logging 
        // being active, instead of logging the message directly.
        if( logging && log.isDebugEnabled() ) {
            logWriter.logOneWay(log, command);
        }
        next.oneway(command);
    }

    public void onCommand(Object command) {
        // Changed this method to use a LogWriter object to actually 
        // print the messages to the log, and only in case of logging 
        // being active, instead of logging the message directly.
        if( logging && log.isDebugEnabled() ) {
            logWriter.logReceivedCommand(log, command);
        }
        getTransportListener().onCommand(command);
    }

    public void onException(IOException error) {
        // Changed this method to use a LogWriter object to actually 
        // print the messages to the log, and only in case of logging 
        // being active, instead of logging the message directly.
        if( logging && log.isDebugEnabled() ) {
            logWriter.logReceivedException(log, error);
        }
        getTransportListener().onException(error);
    }

    /**
     * Gets the associated MBean for this TransportLogger.
     * @return the associated MBean for this TransportLogger.
     */
    public TransportLoggerView getView() {
        return view;
    }

    /**
     * Sets the associated MBean for this TransportLogger.
     * @param view the associated MBean for this TransportLogger.
     */
    public void setView(TransportLoggerView view) {
        this.view = view;
    }


    public String toString() {
        return next.toString();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
        if (view != null) {
            view.unregister();
        }
    }
}
