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
package org.apache.activemq.broker;

import java.io.IOException;

import org.apache.activemq.broker.ft.MasterBroker;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @version $Revision: 1.8 $
 */
public class TransportConnection extends AbstractConnection {
    private static final Log log = LogFactory.getLog(TransportConnection.class);
    private final Transport transport;
    private boolean slow;
    private boolean markedCandidate;
    private boolean blockedCandidate;
    private boolean blocked;
    private boolean connected;
    private boolean active;
    private long timeStamp=0;
    private MasterBroker masterBroker; //used if this connection is used by a Slave

    /**
     * @param connector
     * @param transport
     * @param broker
     * @param taskRunnerFactory - can be null if you want direct dispatch to the transport else commands are sent async.
     */
    public TransportConnection(TransportConnector connector, final Transport transport, Broker broker, TaskRunnerFactory taskRunnerFactory) {
        super(connector, broker, taskRunnerFactory);
        connector.setBrokerName(broker.getBrokerName());
        this.transport = transport;
        this.transport.setTransportListener(new DefaultTransportListener() {
            public void onCommand(Command command) {
                Response response = service(command);
                if( response!=null ) {
                    dispatch(response);
                }
            }
            public void onException(IOException exception) {
                serviceTransportException(exception);
            }
        });
        connected = true;
    }

    public void start() throws Exception {
        transport.start();
        active = true;
        super.start();
        connector.onStarted(this);
    }

    public void stop() throws Exception {
        connector.onStopped(this);
        try {
            if (masterBroker != null){
                masterBroker.stop();
            }
            transport.oneway(new ShutdownInfo());
        } catch (Exception ignore) {
            //ignore.printStackTrace();
        }

        transport.stop();
        active = false;
        super.stop();
    }
    
    
    /**
     * @return Returns the blockedCandidate.
     */
    public boolean isBlockedCandidate(){
        return blockedCandidate;
    }
    /**
     * @param blockedCandidate
     *            The blockedCandidate to set.
     */
    public void setBlockedCandidate(boolean blockedCandidate){
        this.blockedCandidate=blockedCandidate;
    }
    /**
     * @return Returns the markedCandidate.
     */
    public boolean isMarkedCandidate(){
        return markedCandidate;
    }
    /**
     * @param markedCandidate
     *            The markedCandidate to set.
     */
    public void setMarkedCandidate(boolean markedCandidate){
        this.markedCandidate=markedCandidate;
        if(!markedCandidate){
            timeStamp=0;
            blockedCandidate=false;
        }
    }
    /**
     * @param slow
     *            The slow to set.
     */
    public void setSlow(boolean slow){
        this.slow=slow;
    }
    /**
     * @return true if the Connection is slow
     */
    public boolean isSlow(){
        return slow;
    }
    /**
     * @return true if the Connection is potentially blocked
     */
    public boolean isMarkedBlockedCandidate(){
        return markedCandidate;
    }
    
    /**
     * Mark the Connection, so we can deem if it's collectable on the next sweep
     */
    public void doMark(){
        if(timeStamp==0){
            timeStamp=System.currentTimeMillis();
        }
    }
    /**
     * @return if after being marked, the Connection is still writing
     */
    public boolean isBlocked(){
        return blocked;
    }
    /**
     * @return true if the Connection is connected
     */
    public boolean isConnected(){
        return connected;
    }
    /**
     * @param blocked
     *            The blocked to set.
     */
    public void setBlocked(boolean blocked){
        this.blocked=blocked;
    }
    /**
     * @param connected
     *            The connected to set.
     */
    public void setConnected(boolean connected){
        this.connected=connected;
    }
    /**
     * @return true if the Connection is active
     */
    public boolean isActive(){
        return active;
    }
    /**
     * @param active
     *            The active to set.
     */
    public void setActive(boolean active){
        this.active=active;
    }
    
    public Response processBrokerInfo(BrokerInfo info){
        if(info.isSlaveBroker()){
            //stream messages from this broker (the master) to 
            //the slave
            MutableBrokerFilter parent=(MutableBrokerFilter) broker.getAdaptor(MutableBrokerFilter.class);
            masterBroker=new MasterBroker(parent,transport);
            masterBroker.startProcessing();
            log.info("Slave Broker "+info.getBrokerName()+" is attached");
        }
        return super.processBrokerInfo(info);
    }

    protected void dispatch(Command command){
        try{
            setMarkedCandidate(true);
            transport.oneway(command);
            getStatistics().onCommand(command);
        }catch(IOException e){
            serviceException(e);
        }finally{
            setMarkedCandidate(false);
        }
    }
}
