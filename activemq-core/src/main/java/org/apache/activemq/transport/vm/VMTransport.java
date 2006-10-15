/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.transport.vm;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.activemq.command.Command;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportDisposedIOException;
import org.apache.activemq.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicLong;
/**
 * A Transport implementation that uses direct method invocations.
 * 
 * @version $Revision$
 */
public class VMTransport implements Transport,Task{
    private static final Log log=LogFactory.getLog(VMTransport.class);
    private static final AtomicLong nextId=new AtomicLong(0);
    private static final TaskRunnerFactory taskRunnerFactory=new TaskRunnerFactory("VMTransport",Thread.NORM_PRIORITY,
                    true,1000);
    protected VMTransport peer;
    protected TransportListener transportListener;
    protected boolean disposed;
    protected boolean marshal;
    protected boolean network;
    protected boolean async=false;
    protected boolean started=false;
    protected int asyncQueueDepth=2000;
    protected List prePeerSetQueue=Collections.synchronizedList(new LinkedList());
    protected LinkedBlockingQueue messageQueue;
    protected final URI location;
    protected final long id;
    private TaskRunner taskRunner;

    public VMTransport(URI location){
        this.location=location;
        this.id=nextId.getAndIncrement();
    }

    synchronized public VMTransport getPeer(){
        return peer;
    }

    synchronized public void setPeer(VMTransport peer){
        this.peer=peer;
    }

    public void oneway(Object command) throws IOException{
        if(disposed){
            throw new TransportDisposedIOException("Transport disposed.");
        }
        if(peer==null)
            throw new IOException("Peer not connected.");
        if(!peer.disposed){
           
            if(async){
               asyncOneWay(command); 
            }else{
                syncOneWay(command);
            }
        }else{
            throw new TransportDisposedIOException("Peer ("+peer.toString()+") disposed.");
        }
    }
    
    protected void syncOneWay(Object command){
        final TransportListener tl=peer.transportListener;
        prePeerSetQueue=peer.prePeerSetQueue;
        if(tl==null){
            prePeerSetQueue.add(command);
        }else{
            tl.onCommand(command);
        }
    }
    
    protected void asyncOneWay(Object command) throws IOException{
        messageQueue=getMessageQueue();
        try{
            messageQueue.put(command);
            wakeup();
        }catch(final InterruptedException e){
            log.error("messageQueue interupted",e);
            throw new IOException(e.getMessage());
        }
    }

    public FutureResponse asyncRequest(Object command,ResponseCallback responseCallback) throws IOException{
        throw new AssertionError("Unsupported Method");
    }

    public Object request(Object command) throws IOException{
        throw new AssertionError("Unsupported Method");
    }

    public Object request(Object command,int timeout) throws IOException{
        throw new AssertionError("Unsupported Method");
    }

    public synchronized TransportListener getTransportListener(){
        return transportListener;
    }

    synchronized public void setTransportListener(TransportListener commandListener){
        this.transportListener=commandListener;
        wakeup();
        peer.wakeup();
    }

    public synchronized void start() throws Exception{
        started=true;
        if(transportListener==null)
            throw new IOException("TransportListener not set.");
        if(!async){
            for(Iterator iter=prePeerSetQueue.iterator();iter.hasNext();){
                Command command=(Command) iter.next();
                transportListener.onCommand(command);
                iter.remove();
            }
        }else{
            wakeup();
            peer.wakeup();
        }
    }

    public void stop() throws Exception{
        started=false;
        if(!disposed){
            disposed=true;
        }
        if(taskRunner!=null){
            taskRunner.shutdown();
            taskRunner=null;
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

    public String toString(){
        return location+"#"+id;
    }

    public String getRemoteAddress(){
        if(peer!=null){
            return peer.toString();
        }
        return null;
    }

    /**
     * @see org.apache.activemq.thread.Task#iterate()
     */
    public boolean iterate(){
        final TransportListener tl=peer.transportListener;
        if(!messageQueue.isEmpty()&&!peer.disposed&&tl!=null){
            final Command command=(Command) messageQueue.poll();
            tl.onCommand(command);
        }
        return !messageQueue.isEmpty()&&!peer.disposed&&!(peer.transportListener==null);
    }

    /**
     * @return the async
     */
    public boolean isAsync(){
        return async;
    }

    /**
     * @param async the async to set
     */
    public void setAsync(boolean async){
        this.async=async;
    }

    /**
     * @return the asyncQueueDepth
     */
    public int getAsyncQueueDepth(){
        return asyncQueueDepth;
    }

    /**
     * @param asyncQueueDepth the asyncQueueDepth to set
     */
    public void setAsyncQueueDepth(int asyncQueueDepth){
        this.asyncQueueDepth=asyncQueueDepth;
    }

    protected void wakeup(){
        if(async&&messageQueue!=null&&!messageQueue.isEmpty()){
            if(taskRunner==null){
                taskRunner=taskRunnerFactory.createTaskRunner(this,"VMTransport: "+toString());
            }
            try{
                taskRunner.wakeup();
            }catch(InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
    }

    protected synchronized LinkedBlockingQueue getMessageQueue(){
        if(messageQueue==null){
            messageQueue=new LinkedBlockingQueue(this.asyncQueueDepth);
        }
        return messageQueue;
    }
}
