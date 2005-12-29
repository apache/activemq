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
 * Creates a {@see org.activeio.RequestChannel} out of a {@see org.activeio.AsynchChannel}.  This 
 * {@see org.activeio.RequestChannel} is thread safe and mutiplexes concurrent requests and responses over
 * the underlying {@see org.activeio.AsynchChannel}.
 * 
 * @version $Revision: 1.4 $
 */
final public class ResponseCorrelator extends TransportFilter {
    
    private static final Log log = LogFactory.getLog(ResponseCorrelator.class);
    
    private final ConcurrentHashMap requestMap = new ConcurrentHashMap();
    private short lastCommandId = 0;

    synchronized short getNextCommandId() {
        return ++lastCommandId;
    }
    
    public ResponseCorrelator(Transport next) {
        super(next);
    }
    
    public void oneway(Command command) throws IOException {
        command.setCommandId(getNextCommandId());
        command.setResponseRequired(false);
        next.oneway(command);
    }

    public FutureResponse asyncRequest(Command command) throws IOException {
        command.setCommandId(getNextCommandId());
        command.setResponseRequired(true);
        FutureResponse future = new FutureResponse();
        requestMap.put(new Short(command.getCommandId()), future);
        next.oneway(command);
        return future;
    }
    
    public Response request(Command command) throws IOException { 
        FutureResponse response = asyncRequest(command);
        return response.getResult();
    }
    
    public void onCommand(Command command) {
        boolean debug = log.isDebugEnabled();
        if( command.isResponse() ) {
            Response response = (Response) command;
            FutureResponse future = (FutureResponse) requestMap.remove(new Short(response.getCorrelationId()));
            if( future!=null ) {
                future.set(response);
            } else {
                if( debug ) log.debug("Received unexpected response for command id: "+response.getCorrelationId());
            }
        } else {
            commandListener.onCommand(command);
        }
    }
    
    public String toString() {
        return next.toString();
    }

}
