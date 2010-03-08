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
     * @return true if the transport is connected
     */
    boolean isConnected();
    
    /**
     * @return true if reconnect is supported
     */
    boolean isReconnectSupported();
    
    /**
     * @return true if updating uris is supported
     */
    boolean isUpdateURIsSupported();
    /**
     * reconnect to another location
     * @param uri
     * @throws IOException on failure of if not supported
     */
    void reconnect(URI uri) throws IOException;
    
    /**
     * Provide a list of available alternative locations
     * @param rebalance 
     * @param uris
     * @throws IOException
     */
    void updateURIs(boolean rebalance,URI[] uris) throws IOException;

    /**
     * Returns a counter which gets incremented as data is read from the transport.
     * It should only be used to determine if there is progress being made in reading the next command from the transport.  
     * The value may wrap into the negative numbers. 
     * 
     * @return a counter which gets incremented as data is read from the transport.
     */
    int getReceiveCounter();    
}
