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
import java.net.URI;

import org.apache.activemq.Service;

/**
 * Represents the client side of a transport allowing messages to be sent
 * synchronously, asynchronously and consumed.
 * 
 * @version $Revision: 1.5 $
 */
public interface Transport extends Service {

    /**
     * A one way asynchronous send
     * 
     * @param command
     * @throws IOException
     */
    void oneway(Object command) throws IOException;

    /**
     * An asynchronous request response where the Receipt will be returned in
     * the future. If responseCallback is not null, then it will be called when
     * the response has been completed.
     * 
     * @param command
     * @param responseCallback TODO
     * @return the FutureResponse
     * @throws IOException
     */
    FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException;

    /**
     * A synchronous request response
     * 
     * @param command
     * @return the response
     * @throws IOException
     */
    Object request(Object command) throws IOException;

    /**
     * A synchronous request response
     * 
     * @param command
     * @param timeout
     * @return the repsonse or null if timeout
     * @throws IOException
     */
    Object request(Object command, int timeout) throws IOException;

    // /**
    // * A one way asynchronous send
    // * @param command
    // * @throws IOException
    // */
    // void oneway(Command command) throws IOException;
    //
    // /**
    // * An asynchronous request response where the Receipt will be returned
    // * in the future. If responseCallback is not null, then it will be called
    // * when the response has been completed.
    // *
    // * @param command
    // * @param responseCallback TODO
    // * @return the FutureResponse
    // * @throws IOException
    // */
    // FutureResponse asyncRequest(Command command, ResponseCallback
    // responseCallback) throws IOException;
    //    
    // /**
    // * A synchronous request response
    // * @param command
    // * @return the response
    // * @throws IOException
    // */
    // Response request(Command command) throws IOException;
    //
    // /**
    // * A synchronous request response
    // * @param command
    // * @param timeout
    // * @return the repsonse or null if timeout
    // * @throws IOException
    // */
    // Response request(Command command, int timeout) throws IOException;

    /**
     * Returns the current transport listener
     * 
     * @return
     */
    TransportListener getTransportListener();

    /**
     * Registers an inbound command listener
     * 
     * @param commandListener
     */
    void setTransportListener(TransportListener commandListener);

    /**
     * @param target
     * @return the target
     */
    <T> T narrow(Class<T> target);

    /**
     * @return the remote address for this connection
     */
    String getRemoteAddress();

    /**
     * Indicates if the transport can handle faults
     * 
     * @return true if fault tolerant
     */
    boolean isFaultTolerant();
    
    /**
     * @return true if the transport is disposed
     */
    boolean isDisposed();
    
    /**
     * reconnect to another location
     * @param uri
     * @throws IOException on failure of if not supported
     */
    void reconnect(URI uri) throws IOException;

}
