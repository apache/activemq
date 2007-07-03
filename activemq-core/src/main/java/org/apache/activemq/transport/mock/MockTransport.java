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
package org.apache.activemq.transport.mock;

import java.io.IOException;

import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportListener;


/**
 * @version $Revision: 1.5 $
 */
public class MockTransport extends DefaultTransportListener implements Transport {

    protected Transport next;
    protected TransportListener transportListener;

    public MockTransport(Transport next) {
        this.next = next;
    }

    /**
     */
    synchronized public void setTransportListener(TransportListener channelListener) {
        this.transportListener = channelListener;
        if (channelListener == null)
            getNext().setTransportListener(null);
        else
            getNext().setTransportListener(this);
    }


    /**
     * @see org.apache.activemq.Service#start()
     * @throws IOException if the next channel has not been set.
     */
    public void start() throws Exception {
        if( getNext() == null )
            throw new IOException("The next channel has not been set.");
        if( transportListener == null )
            throw new IOException("The command listener has not been set.");
        getNext().start();
    }

    /**
     * @see org.apache.activemq.Service#stop()
     */
    public void stop() throws Exception {
        getNext().stop();
    }    

    public void onCommand(Object command) {
        getTransportListener().onCommand(command);
    }

    /**
     * @return Returns the getNext().
     */
    synchronized public Transport getNext() {
        return next;
    }

    /**
     * @return Returns the packetListener.
     */
    synchronized public TransportListener getTransportListener() {
        return transportListener;
    }
    
    public String toString() {
        return getNext().toString();
    }

    public void oneway(Object command) throws IOException {
        getNext().oneway(command);
    }

    public FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException {
        return getNext().asyncRequest(command, null);
    }

    public Object request(Object command) throws IOException {
        return getNext().request(command);
    }
    
    public Object request(Object command,int timeout) throws IOException {
        return getNext().request(command, timeout);
    }

    public void onException(IOException error) {
        getTransportListener().onException(error);
    }

    public Object narrow(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return getNext().narrow(target);
    }

    synchronized public void setNext(Transport next) {
        this.next = next;
    }

    public void install(TransportFilter filter) {
        filter.setTransportListener(this);
        getNext().setTransportListener(filter);
        setNext(filter);
    }

	public String getRemoteAddress() {
		return getNext().getRemoteAddress();
	}

    /**
     * @see org.apache.activemq.transport.Transport#isFaultTolerant()
     */
    public boolean isFaultTolerant(){
       return getNext().isFaultTolerant();
    }  
    
}
