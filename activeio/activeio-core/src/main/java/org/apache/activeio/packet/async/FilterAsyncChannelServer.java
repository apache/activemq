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
package org.apache.activeio.packet.async;

import java.io.IOException;
import java.net.URI;

import org.apache.activeio.AcceptListener;
import org.apache.activeio.Channel;


/**
 * A AsyncChannelFilter can be used as a filter between a {@see org.apache.activeio.AsyncChannel}
 * and it's {@see org.apache.activeio.ChannelConsumer}.  Most {@see org.apache.activeio.AsyncChannel}
 * that are not directly accessing the network will extends the AsyncChannelFilter since they act as a
 * filter between the client and the network.  O 
 * 
 * @version $Revision$
 */
public class FilterAsyncChannelServer implements AsyncChannelServer, AcceptListener {

    final protected AsyncChannelServer next;
    protected AcceptListener acceptListener;

    public FilterAsyncChannelServer(AsyncChannelServer next) {
        this.next = next;
        if( next == null )
            throw new IllegalArgumentException("The next AsyncChannelServer cannot be null.");
    }

    public void setAcceptListener(AcceptListener acceptListener) {
        this.acceptListener = acceptListener;
        if (acceptListener == null)
            next.setAcceptListener(null);
        else
            next.setAcceptListener(this);
        
    }
    
    /**
     * @see org.apache.activeio.Disposable#dispose()
     */
    public void dispose() {
        next.dispose();
    }

    /**
     * @see org.apache.activeio.Service#start()
     * @throws IOException if the next channel has not been set.
     */
    public void start() throws IOException {
        if( acceptListener ==null )
            throw new IOException("The AcceptListener has not been set.");
        next.start();
    }

    /**
     * @see org.apache.activeio.Service#stop()
     */
    public void stop() throws IOException {
        next.stop();
    }

    public void onAccept(Channel channel) {
        acceptListener.onAccept(channel);
    }

    public void onAcceptError(IOException error) {
        acceptListener.onAcceptError(error);
    }

    public URI getBindURI() {
        return next.getBindURI();
    }

    public URI getConnectURI() {
        return next.getConnectURI();
    }
    
    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return next.getAdapter(target);
    }    
    
    public String toString() {
        return next.toString();
    }
 }