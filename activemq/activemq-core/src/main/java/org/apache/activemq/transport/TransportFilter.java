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


/**
 * @version $Revision: 1.5 $
 */
public class TransportFilter extends DefaultTransportListener implements Transport {

    final protected Transport next;
    private TransportListener transportListener;

    public TransportFilter(Transport next) {
        this.next = next;
    }

    public TransportListener getTransportListener() {
        return transportListener;
    }
    
    public void setTransportListener(TransportListener channelListener) {
        this.transportListener = channelListener;
        if (channelListener == null)
            next.setTransportListener(null);
        else
            next.setTransportListener(this);
    }


    /**
     * @see org.apache.activemq.Service#start()
     * @throws IOException if the next channel has not been set.
     */
    public void start() throws Exception {
        if( next == null )
            throw new IOException("The next channel has not been set.");
        if( transportListener == null )
            throw new IOException("The command listener has not been set.");
        next.start();
    }

    /**
     * @see org.apache.activemq.Service#stop()
     */
    public void stop() throws Exception {
        next.stop();
    }    

    public void onCommand(Command command) {
        transportListener.onCommand(command);
    }

    /**
     * @return Returns the next.
     */
    public Transport getNext() {
        return next;
    }


    public String toString() {
        return next.toString();
    }

    public void oneway(Command command) throws IOException {
        next.oneway(command);
    }

    public FutureResponse asyncRequest(Command command, ResponseCallback responseCallback) throws IOException {
        return next.asyncRequest(command, null);
    }

    public Response request(Command command) throws IOException {
        return next.request(command);
    }
    
    public Response request(Command command,int timeout) throws IOException {
        return next.request(command,timeout);
    }

    public void onException(IOException error) {
        transportListener.onException(error);
    }

    public Object narrow(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return next.narrow(target);
    }  
    
}