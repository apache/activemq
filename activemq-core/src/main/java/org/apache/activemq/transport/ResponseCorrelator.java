/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Response;
import org.apache.activemq.util.IntSequenceGenerator;
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
    private IntSequenceGenerator sequenceGenerator;
    
    public ResponseCorrelator(Transport next) {
        this(next, new IntSequenceGenerator());
    }
    
    public ResponseCorrelator(Transport next, IntSequenceGenerator sequenceGenerator) {
        super(next);
        this.sequenceGenerator = sequenceGenerator;
    }

    public void oneway(Object o) throws IOException {
    	Command command = (Command) o;
        command.setCommandId(sequenceGenerator.getNextSequenceId());
        command.setResponseRequired(false);
        next.oneway(command);
    }

    public FutureResponse asyncRequest(Object o, ResponseCallback responseCallback) throws IOException {
    	Command command = (Command) o;
        command.setCommandId(sequenceGenerator.getNextSequenceId());
        command.setResponseRequired(true);
        FutureResponse future = new FutureResponse(responseCallback);
        requestMap.put(new Integer(command.getCommandId()), future);
        next.oneway(command);
        return future;
    }
    
    public Object request(Object command) throws IOException { 
        FutureResponse response = asyncRequest(command, null);
        return response.getResult();
    }
    
    public Object request(Object command,int timeout) throws IOException {
        FutureResponse response = asyncRequest(command, null);
        return response.getResult(timeout);
    }
    
    public void onCommand(Object o) {
    	Command command = (Command) o;
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
    
    /**
     * If an async exception occurs, then assume no responses will arrive for any of
     * current requests.  Lets let them know of the problem.
     */
    public void onException(IOException error) {
        
        // Copy and Clear the request Map
        ArrayList requests = new ArrayList(requestMap.values());
        requestMap.clear();
        
        for (Iterator iter = requests.iterator(); iter.hasNext();) {
            FutureResponse fr = (FutureResponse) iter.next();
            fr.set(new ExceptionResponse(error));
        }
        
        super.onException(error);
    }
    
    public IntSequenceGenerator getSequenceGenerator() {
        return sequenceGenerator;
    }

    public String toString() {
        return next.toString();
    }

}
