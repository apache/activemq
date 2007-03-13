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

package org.apache.activemq.network;

import static org.apache.activemq.network.NetworkConnector.log;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.activemq.Service;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision$
 */
public abstract class NetworkConnector extends NetworkBridgeConfiguration implements Service{

    protected static final Log log=LogFactory.getLog(NetworkConnector.class);
    protected URI localURI;
    private String brokerName="localhost";
    private Set durableDestinations;
    private List excludedDestinations=new CopyOnWriteArrayList();
    private List dynamicallyIncludedDestinations=new CopyOnWriteArrayList();
    private List staticallyIncludedDestinations=new CopyOnWriteArrayList();
    private String name="bridge";
    protected ConnectionFilter connectionFilter;
    protected ServiceSupport serviceSupport=new ServiceSupport(){

        protected void doStart() throws Exception{
           handleStart();
        }

        protected void doStop(ServiceStopper stopper) throws Exception{
            handleStop(stopper);
        }
    };

    public NetworkConnector(){
    }

    public NetworkConnector(URI localURI){
        this.localURI=localURI;
    }

    public URI getLocalUri() throws URISyntaxException{
        return localURI;
    }

    public void setLocalUri(URI localURI){
        this.localURI=localURI;
    }

    /**
     * @return Returns the name.
     */
    public String getName(){
        if(name==null){
            name=createName();
        }
        return name;
    }

    /**
     * @param name The name to set.
     */
    public void setName(String name){
        this.name=name;
    }

    public String getBrokerName(){
        return brokerName;
    }

    /**
     * @param brokerName The brokerName to set.
     */
    public void setBrokerName(String brokerName){
        this.brokerName=brokerName;
    }

    /**
     * @return Returns the durableDestinations.
     */
    public Set getDurableDestinations(){
        return durableDestinations;
    }

    /**
     * @param durableDestinations The durableDestinations to set.
     */
    public void setDurableDestinations(Set durableDestinations){
        this.durableDestinations=durableDestinations;
    }

    /**
     * @return Returns the excludedDestinations.
     */
    public List getExcludedDestinations(){
        return excludedDestinations;
    }

    /**
     * @param excludedDestinations The excludedDestinations to set.
     */
    public void setExcludedDestinations(List exludedDestinations){
        this.excludedDestinations=exludedDestinations;
    }

    public void addExcludedDestination(ActiveMQDestination destiantion){
        this.excludedDestinations.add(destiantion);
    }

    /**
     * @return Returns the staticallyIncludedDestinations.
     */
    public List getStaticallyIncludedDestinations(){
        return staticallyIncludedDestinations;
    }

    /**
     * @param staticallyIncludedDestinations The staticallyIncludedDestinations to set.
     */
    public void setStaticallyIncludedDestinations(List staticallyIncludedDestinations){
        this.staticallyIncludedDestinations=staticallyIncludedDestinations;
    }

    public void addStaticallyIncludedDestination(ActiveMQDestination destiantion){
        this.staticallyIncludedDestinations.add(destiantion);
    }

    /**
     * @return Returns the dynamicallyIncludedDestinations.
     */
    public List getDynamicallyIncludedDestinations(){
        return dynamicallyIncludedDestinations;
    }

    /**
     * @param dynamicallyIncludedDestinations The dynamicallyIncludedDestinations to set.
     */
    public void setDynamicallyIncludedDestinations(List dynamicallyIncludedDestinations){
        this.dynamicallyIncludedDestinations=dynamicallyIncludedDestinations;
    }

    public void addDynamicallyIncludedDestination(ActiveMQDestination destiantion){
        this.dynamicallyIncludedDestinations.add(destiantion);
    }
    
    public ConnectionFilter getConnectionFilter(){
        return connectionFilter;
    }

    public void setConnectionFilter(ConnectionFilter connectionFilter){
        this.connectionFilter=connectionFilter;
    }


    // Implementation methods
    // -------------------------------------------------------------------------
    protected NetworkBridge configureBridge(DemandForwardingBridgeSupport result){
        result.setName(getBrokerName());
        List destsList=getDynamicallyIncludedDestinations();
        ActiveMQDestination dests[]=(ActiveMQDestination[])destsList.toArray(new ActiveMQDestination[destsList.size()]);
        result.setDynamicallyIncludedDestinations(dests);
        destsList=getExcludedDestinations();
        dests=(ActiveMQDestination[])destsList.toArray(new ActiveMQDestination[destsList.size()]);
        result.setExcludedDestinations(dests);
        destsList=getStaticallyIncludedDestinations();
        dests=(ActiveMQDestination[])destsList.toArray(new ActiveMQDestination[destsList.size()]);
        result.setStaticallyIncludedDestinations(dests);
        if(durableDestinations!=null){
            ActiveMQDestination[] dest=new ActiveMQDestination[durableDestinations.size()];
            dest=(ActiveMQDestination[])durableDestinations.toArray(dest);
            result.setDurableDestinations(dest);
        }
        return result;
    }

    protected abstract String createName();

    protected Transport createLocalTransport() throws Exception{
        return TransportFactory.connect(localURI);
    }

    public void start() throws Exception{
        serviceSupport.start();
    }

    public void stop() throws Exception{
        serviceSupport.stop();
    }
    
    protected void handleStart() throws Exception{
        if(localURI==null){
            throw new IllegalStateException("You must configure the 'localURI' property");
        }
        log.info("Network Connector "+getName()+" Started");
    }

    protected void handleStop(ServiceStopper stopper) throws Exception{
        log.info("Network Connector "+getName()+" Stopped");
    }
}
