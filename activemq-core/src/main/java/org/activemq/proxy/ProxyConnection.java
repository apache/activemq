/** 
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 * 
 * Copyright 2005 Hiram Chirino
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
 * 
 **/

package org.activemq.proxy;

import java.io.IOException;

import org.activemq.Service;
import org.activemq.command.Command;
import org.activemq.command.ShutdownInfo;
import org.activemq.transport.Transport;
import org.activemq.transport.TransportListener;
import org.activemq.util.IOExceptionSupport;
import org.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

class ProxyConnection implements Service {

    static final private Log log = LogFactory.getLog(ProxyConnection.class);
    
    private final Transport localTransport;
    private final Transport remoteTransport;
    private AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private AtomicBoolean running = new AtomicBoolean(false);

    public ProxyConnection(Transport localTransport, Transport remoteTransport) {
        this.localTransport = localTransport;
        this.remoteTransport = remoteTransport;
    }

    public void onFailure(IOException e) {
        if( !shuttingDown.get() ) {
            log.debug("Transport error: "+e,e);
            try {
                stop();
            } catch (Exception ignore) {
            }
        }
    }

    public void start() throws Exception {
        if( !running.compareAndSet(false, true) ) {
            return;            
        }
            
        this.localTransport.setTransportListener(new TransportListener() {
            public void onCommand(Command command) {
                boolean shutdown=false;
                if( command.getClass() == ShutdownInfo.class ) {
                    shuttingDown.set(true);
                    shutdown=true;
                }
                try {
                    remoteTransport.oneway(command);
                    if( shutdown )
                        stop();
                } catch (IOException error) {
                    onFailure(error);
                } catch (Exception error) {
                    onFailure(IOExceptionSupport.create(error));
                }
            }
            public void onException(IOException error) {
                onFailure(error);
            }
        });
        
        this.remoteTransport.setTransportListener(new TransportListener() {
            public void onCommand(Command command) {
                try {
                    localTransport.oneway(command);
                } catch (IOException error) {
                    onFailure(error);
                }
            }
            public void onException(IOException error) {
                onFailure(error);
            }
        });
        
        localTransport.start();
        remoteTransport.start();
    }
    
    public void stop() throws Exception {
        if( !running.compareAndSet(true, false) ) {
            return;
        }
        shuttingDown.set(true);
        ServiceStopper ss = new ServiceStopper();
        ss.stop(localTransport);
        ss.stop(remoteTransport);
        ss.throwFirstException();
    }
    
}