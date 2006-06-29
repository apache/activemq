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
package org.apache.activemq.transport.discovery.multicast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicLong;
/**
 * A {@link DiscoveryAgent} using <a href="http://www.zeroconf.org/">Zeroconf</a> via the <a
 * href="http://jmdns.sf.net/">jmDNS</a> library
 * 
 * @version $Revision$
 */
public class MulticastDiscoveryAgent implements DiscoveryAgent,Runnable{
    private static final Log log=LogFactory.getLog(MulticastDiscoveryAgent.class);
    public static final String DEFAULT_DISCOVERY_URI_STRING="multicast://224.1.2.3:6155";
    private static final String TYPE_SUFFIX="ActiveMQ-4.";
    private static final String ALIVE="alive.";
    private static final String DEAD="dead.";
    private static final String DELIMITER = "%";
    private static final int BUFF_SIZE=8192;
    private static final int DEFAULT_IDLE_TIME=500;
    private static final int HEARTBEAT_MISS_BEFORE_DEATH=4;
    private int timeToLive=1;
    private boolean loopBackMode=false;
    private Map services=new ConcurrentHashMap();
    private Map brokers = new ConcurrentHashMap();
    private String group="default";
    private String brokerName;
    private URI discoveryURI;
    private InetAddress inetAddress;
    private SocketAddress sockAddress;
    private DiscoveryListener discoveryListener;
    private String selfService;
    private MulticastSocket mcast;
    private Thread runner;
    private long keepAliveInterval=DEFAULT_IDLE_TIME;
    private long lastAdvertizeTime=0;
    private AtomicBoolean started=new AtomicBoolean(false);
    private boolean reportAdvertizeFailed=true;
    
    private final Executor executor = new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS, new LinkedBlockingQueue(), new ThreadFactory() {
        public Thread newThread(Runnable runable) {
            Thread t = new Thread(runable, "Multicast Discovery Agent Notifier");
            t.setDaemon(true);
            return t;
        }            
    });

    /**
     * Set the discovery listener
     * 
     * @param listener
     */
    public void setDiscoveryListener(DiscoveryListener listener){
        this.discoveryListener=listener;
    }

    /**
     * register a service
     */
    public void registerService(String name) throws IOException{
        this.selfService=name;
        if (started.get()){
            doAdvertizeSelf();
        }
    }

    /**
     * Get the group used for discovery
     * 
     * @return the group
     */
    public String getGroup(){
        return group;
    }

    /**
     * Set the group for discovery
     * 
     * @param group
     */
    public void setGroup(String group){
        this.group=group;
    }

    /**
     * @return Returns the brokerName.
     */
    public String getBrokerName(){
        return brokerName;
    }

    /**
     * @param brokerName The brokerName to set.
     */
    public void setBrokerName(String brokerName){
        if (brokerName != null){
            brokerName = brokerName.replace('.','-');
            brokerName = brokerName.replace(':','-');
            brokerName = brokerName.replace('%','-');
        this.brokerName=brokerName;
        }
    }

    /**
     * @return Returns the loopBackMode.
     */
    public boolean isLoopBackMode(){
        return loopBackMode;
    }

    /**
     * @param loopBackMode
     *            The loopBackMode to set.
     */
    public void setLoopBackMode(boolean loopBackMode){
        this.loopBackMode=loopBackMode;
    }

    /**
     * @return Returns the timeToLive.
     */
    public int getTimeToLive(){
        return timeToLive;
    }

    /**
     * @param timeToLive
     *            The timeToLive to set.
     */
    public void setTimeToLive(int timeToLive){
        this.timeToLive=timeToLive;
    }

    /**
     * @return the discoveryURI
     */
    public URI getDiscoveryURI(){
        return discoveryURI;
    }

    /**
     * Set the discoveryURI
     * 
     * @param discoveryURI
     */
    public void setDiscoveryURI(URI discoveryURI){
        this.discoveryURI=discoveryURI;
    }

    public long getKeepAliveInterval(){
        return keepAliveInterval;
    }

    public void setKeepAliveInterval(long keepAliveInterval){
        this.keepAliveInterval=keepAliveInterval;
    }

    /**
     * start the discovery agent
     * 
     * @throws Exception
     */
    public void start() throws Exception{
        if(started.compareAndSet(false,true)){
            if(group==null|| group.length()==0){
                throw new IOException("You must specify a group to discover");
            }
            if (brokerName == null || brokerName.length()==0){
                log.warn("brokerName not set");
            }
            String type=getType();
            if(!type.endsWith(".")){
                log.warn("The type '"+type+"' should end with '.' to be a valid Discovery type");
                type+=".";
            }
            if(discoveryURI==null){
                discoveryURI=new URI(DEFAULT_DISCOVERY_URI_STRING);
            }
            this.inetAddress=InetAddress.getByName(discoveryURI.getHost());
            this.sockAddress=new InetSocketAddress(this.inetAddress,discoveryURI.getPort());
            mcast=new MulticastSocket(discoveryURI.getPort());
            mcast.setLoopbackMode(loopBackMode);
            mcast.setTimeToLive(getTimeToLive());
            mcast.joinGroup(inetAddress);
            mcast.setSoTimeout((int) keepAliveInterval);
            runner=new Thread(this);
            runner.setName("MulticastDiscovery: "+selfService);
            runner.setDaemon(true);
            runner.start();
            doAdvertizeSelf();
        }
    }

    /**
     * stop the channel
     * 
     * @throws Exception
     */
    public void stop() throws Exception{
        if(started.compareAndSet(true,false)){
            doAdvertizeSelf();
            mcast.close();
        }
    }

    public String getType(){
        return group+"."+TYPE_SUFFIX;
    }

    public void run(){
        byte[] buf=new byte[BUFF_SIZE];
        DatagramPacket packet=new DatagramPacket(buf,0,buf.length);
        while(started.get()){
            doTimeKeepingServices();
            try{
                mcast.receive(packet);
                if(packet.getLength()>0){
                    String str=new String(packet.getData(),packet.getOffset(),packet.getLength());
                    processData(str);
                }
            } catch(SocketTimeoutException se){
                // ignore
            } catch(IOException e){
            	if( started.get() ) {
            		log.error("failed to process packet: "+e);
            	}
            }
        }
    }

    private void processData(String str){
        if (discoveryListener != null){
        if(str.startsWith(getType())){
            String payload=str.substring(getType().length());
            if(payload.startsWith(ALIVE)){
                String brokerName=getBrokerName(payload.substring(ALIVE.length()));
                String service=payload.substring(ALIVE.length()+brokerName.length()+2);
                if(!brokerName.equals(this.brokerName)){
                    processAlive(brokerName,service);
                }
            }else{
                String brokerName=getBrokerName(payload.substring(DEAD.length()));
                String service=payload.substring(DEAD.length()+brokerName.length()+2);
                if(!brokerName.equals(this.brokerName)){
                    processDead(brokerName,service);
                }
            }
        }
        }
    }

    private void doTimeKeepingServices(){
        if(started.get()){
            long currentTime=System.currentTimeMillis();
            if((currentTime-keepAliveInterval)>lastAdvertizeTime){
                doAdvertizeSelf();
                lastAdvertizeTime = currentTime;
            }
            doExpireOldServices();
        }
    }

    private void doAdvertizeSelf(){
        if(selfService!=null ){
            String payload=getType();
            payload+=started.get()?ALIVE:DEAD;
            payload+=DELIMITER+brokerName+DELIMITER;
            payload+=selfService;
            try{
                byte[] data=payload.getBytes();
                DatagramPacket packet=new DatagramPacket(data,0,data.length,sockAddress);
                mcast.send(packet);
            } catch(IOException e) {
                // If a send fails, chances are all subsequent sends will fail too.. No need to keep reporting the
                // same error over and over.
                if( reportAdvertizeFailed ) {
                    reportAdvertizeFailed=false;
                    log.error("Failed to advertise our service: "+payload,e);
                    if( "Operation not permitted".equals(e.getMessage()) ) {
                        log.error("The 'Operation not permitted' error has been know to be caused by improper firewall/network setup.  Please make sure that the OS is properly configured to allow multicast traffic over: "+mcast.getLocalAddress());
                    }
                }
            }
        }
    }

    private void processAlive(String brokerName,String service){
        if(selfService == null || !service.equals(selfService)){
            AtomicLong lastKeepAlive=(AtomicLong) services.get(service);
            if(lastKeepAlive==null){
                brokers.put(service, brokerName);
                if(discoveryListener!=null){
                    final DiscoveryEvent event=new DiscoveryEvent(service);
                    event.setBrokerName(brokerName);
                    
                    // Have the listener process the event async so that 
                    // he does not block this thread since we are doing time sensitive
                    // processing of events.
                    executor.execute(new Runnable() {
                        public void run() {
                            DiscoveryListener discoveryListener = MulticastDiscoveryAgent.this.discoveryListener;
                            if(discoveryListener!=null){
                                discoveryListener.onServiceAdd(event);
                            }
                        }
                    });
                }
                lastKeepAlive=new AtomicLong(System.currentTimeMillis());
                services.put(service,lastKeepAlive);
                doAdvertizeSelf();
                
            }
            lastKeepAlive.set(System.currentTimeMillis());
        }
    }

    private void processDead(String brokerName,String service){
        if(!service.equals(selfService)){
            if(services.remove(service)!=null){
                brokers.remove(service);
                if(discoveryListener!=null){
                    final DiscoveryEvent event=new DiscoveryEvent(service);
                    event.setBrokerName(brokerName);
                    
                    // Have the listener process the event async so that 
                    // he does not block this thread since we are doing time sensitive
                    // processing of events.
                    executor.execute(new Runnable() {
                        public void run() {
                            DiscoveryListener discoveryListener = MulticastDiscoveryAgent.this.discoveryListener;
                            if(discoveryListener!=null){
                                discoveryListener.onServiceRemove(event);
                            }
                        }
                    });
                }
            }
        }
    }

    private void doExpireOldServices(){
        long expireTime=System.currentTimeMillis()-(keepAliveInterval*HEARTBEAT_MISS_BEFORE_DEATH);
        for(Iterator i=services.entrySet().iterator();i.hasNext();){
            Map.Entry entry=(Map.Entry) i.next();
            AtomicLong lastHeartBeat=(AtomicLong) entry.getValue();
            if(lastHeartBeat.get()<expireTime){
                String brokerName = (String)brokers.get(entry.getKey());
                processDead(brokerName,entry.getKey().toString());
            }
        }
    }
    
    private String getBrokerName(String str){
        String result = null;
        int start = str.indexOf(DELIMITER);
        if (start >= 0 ){
            int end = str.indexOf(DELIMITER,start+1);
            result=str.substring(start+1, end);
        }
        return result;
    }

    public void serviceFailed(DiscoveryEvent event) throws IOException {
        processDead(event.getBrokerName(), event.getServiceName());
    }
}
