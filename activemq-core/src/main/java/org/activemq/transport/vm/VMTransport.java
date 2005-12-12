/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 * 
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * 
 */
package org.activemq.transport.vm;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.activemq.command.Command;
import org.activemq.command.Response;
import org.activemq.transport.FutureResponse;
import org.activemq.transport.Transport;
import org.activemq.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * A Transport implementation that uses direct method invocations.
 * 
 * @version $Revision$
 */
public class VMTransport implements Transport{
    private static final Log log=LogFactory.getLog(VMTransport.class);
    protected VMTransport peer;
    protected TransportListener transportListener;
    protected boolean disposed;
    protected boolean marshal;
    protected boolean network;
    protected List queue = Collections.synchronizedList(new LinkedList());
    

    synchronized public VMTransport getPeer(){
        return peer;
    }

    synchronized public void setPeer(VMTransport peer){
        this.peer=peer;
    }

    public void oneway(Command command) throws IOException{
        if(disposed)
            throw new IOException("Transport disposed.");
        if(peer==null)
            throw new IOException("Peer not connected.");
        if (!peer.disposed){
            TransportListener tl = peer.transportListener;
            queue = peer.queue;
            if (tl != null){
                tl.onCommand(command);
            }else {
                queue.add(command);
            }
        }
    }

    public FutureResponse asyncRequest(Command command) throws IOException{
        throw new AssertionError("Unsupported Method");
    }

    public Response request(Command command) throws IOException{
        throw new AssertionError("Unsupported Method");
    }

    synchronized public void setTransportListener(TransportListener commandListener){
        this.transportListener=commandListener;
    }

    public synchronized void start() throws Exception{
        if(transportListener==null)
            throw new IOException("TransportListener not set.");
        for (Iterator iter = queue.iterator(); iter.hasNext();) {
            Command command = (Command) iter.next();
            transportListener.onCommand(command);
            iter.remove();
        }
    }

    public void stop() throws Exception{
        if(!disposed){
            disposed=true;
        }
    }

    public Object narrow(Class target){
        if(target.isAssignableFrom(getClass())){
            return this;
        }
        return null;
    }

    public boolean isMarshal(){
        return marshal;
    }

    public void setMarshal(boolean marshal){
        this.marshal=marshal;
    }

    public boolean isNetwork(){
        return network;
    }

    public void setNetwork(boolean network){
        this.network=network;
    }

}
