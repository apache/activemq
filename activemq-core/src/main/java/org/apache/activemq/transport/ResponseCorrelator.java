/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport;

import java.io.IOException;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.Response;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;


/**
 * Adds the incrementing sequence number to commands along with performing the corelation of
 * responses to requests to create a blocking request-response semantics.
 * 
 * @version $Revision: 1.4 $
 */
public class ResponseCorrelator extends TransportFilter {
    
    private static final Log log = LogFactory.getLog(ResponseCorrelator.class);
    
    private final ConcurrentHashMap requestMap = new ConcurrentHashMap();
    private int lastCommandId = 0;

    public synchronized int getNextCommandId() {
        return ++lastCommandId;
    }
    
    public ResponseCorrelator(Transport next) {
        super(next);
    }
    
    public void oneway(Command command) throws IOException {
        // a parent transport could have set the ID
        if (command.getCommandId() == 0) {
            command.setCommandId(getNextCommandId());
        }
        command.setResponseRequired(false);
        next.oneway(command);
    }

    public FutureResponse asyncRequest(Command command) throws IOException {
        // a parent transport could have set the ID
        if (command.getCommandId() == 0) {
            command.setCommandId(getNextCommandId());
        }
        command.setResponseRequired(true);
        FutureResponse future = new FutureResponse();
        requestMap.put(new Integer(command.getCommandId()), future);
        next.oneway(command);
        return future;
    }
    
    public Response request(Command command) throws IOException { 
        FutureResponse response = asyncRequest(command);
        return response.getResult();
    }
    
    public Response request(Command command,int timeout) throws IOException {
        FutureResponse response = asyncRequest(command);
        return response.getResult(timeout);
    }
    
    public void onCommand(Command command) {
        boolean debug = log.isDebugEnabled();
        if( command.isResponse() ) {
            Response response = (Response) command;
            FutureResponse future = (FutureResponse) requestMap.remove(new Integer(response.getCorrelationId()));
            if( future!=null ) {
                future.set(response);
            } else {
                if( debug ) log.debug("Received unexpected response for command id: "+response.getCorrelationId());
            }
        } else {
            getTransportListener().onCommand(command);
        }
    }
    
    public String toString() {
        return next.toString();
    }

}
