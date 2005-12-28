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
package org.apache.activemq.transport;

import java.io.IOException;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.Response;


/**
 * @version $Revision: 1.5 $
 */
public class TransportFilter implements Transport, TransportListener {

    final protected Transport next;
    protected TransportListener commandListener;

    public TransportFilter(Transport next) {
        this.next = next;
    }

    /**
     */
    public void setTransportListener(TransportListener channelListener) {
        this.commandListener = channelListener;
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
        if( commandListener == null )
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
        commandListener.onCommand(command);
    }

    /**
     * @return Returns the next.
     */
    public Transport getNext() {
        return next;
    }

    /**
     * @return Returns the packetListener.
     */
    public TransportListener getCommandListener() {
        return commandListener;
    }
    
    public String toString() {
        return next.toString();
    }

    public void oneway(Command command) throws IOException {
        next.oneway(command);
    }

    public FutureResponse asyncRequest(Command command) throws IOException {
        return next.asyncRequest(command);
    }

    public Response request(Command command) throws IOException {
        return next.request(command);
    }

    public void onException(IOException error) {
        commandListener.onException(error);
    }

    public Object narrow(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return next.narrow(target);
    }  
    
}