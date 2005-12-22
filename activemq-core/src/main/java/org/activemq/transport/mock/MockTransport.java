/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activemq.transport.mock;

import java.io.IOException;

import org.activemq.command.Command;
import org.activemq.command.Response;
import org.activemq.transport.FutureResponse;
import org.activemq.transport.Transport;
import org.activemq.transport.TransportFilter;
import org.activemq.transport.TransportListener;


/**
 * @version $Revision: 1.5 $
 */
public class MockTransport implements Transport, TransportListener {

    protected Transport next;
    protected TransportListener commandListener;

    public MockTransport(Transport next) {
        this.next = next;
    }

    /**
     */
    synchronized public void setTransportListener(TransportListener channelListener) {
        this.commandListener = channelListener;
        if (channelListener == null)
            next.setTransportListener(null);
        else
            next.setTransportListener(this);
    }


    /**
     * @see org.activemq.Service#start()
     * @throws IOException if the next channel has not been set.
     */
    public void start() throws Exception {
        if( next == null )
            throw new IOException("The next channel has not been set.");
        if( commandListener == null )
            throw new IOException("The command listener has not been set.");
        next.start();
    }

    /**
     * @see org.activemq.Service#stop()
     */
    public void stop() throws Exception {
        next.stop();
    }    

    synchronized public void onCommand(Command command) {
        commandListener.onCommand(command);
    }

    /**
     * @return Returns the next.
     */
    synchronized public Transport getNext() {
        return next;
    }

    /**
     * @return Returns the packetListener.
     */
    synchronized public TransportListener getCommandListener() {
        return commandListener;
    }
    
    synchronized public String toString() {
        return next.toString();
    }

    synchronized public void oneway(Command command) throws IOException {
        next.oneway(command);
    }

    synchronized public FutureResponse asyncRequest(Command command) throws IOException {
        return next.asyncRequest(command);
    }

    synchronized public Response request(Command command) throws IOException {
        return next.request(command);
    }

    synchronized public void onException(IOException error) {
        commandListener.onException(error);
    }

    synchronized public Object narrow(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return next.narrow(target);
    }

    synchronized public void setNext(Transport next) {
        this.next = next;
    }

    synchronized public void install(TransportFilter filter) {
        filter.setTransportListener(this);
        getNext().setTransportListener(filter);
        setNext(filter);
    }  
    
}