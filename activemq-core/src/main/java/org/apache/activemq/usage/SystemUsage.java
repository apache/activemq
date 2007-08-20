/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.activemq.Service;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.store.PersistenceAdapter;


/**
 * Holder for Usage instances for memory, store and temp files
 * 
 * Main use case is manage memory usage.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: 1.3 $
 */
public class SystemUsage implements Service{

    private final SystemUsage parent;
    private final String name;
    private final MemoryUsage memoryUsage;
    private final StoreUsage storeUsage;
    private final TempUsage tempDiskUsage;
    /**
     * True if someone called setSendFailIfNoSpace() on this particular usage
     * manager
     */
    private boolean sendFailIfNoSpaceExplicitySet;
    private boolean sendFailIfNoSpace;
    private List<SystemUsage> children=new CopyOnWriteArrayList<SystemUsage>();

    public SystemUsage(){
        this.parent=null;
        this.name="default";
        this.memoryUsage=new MemoryUsage(name+":memory");
        this.storeUsage=null;
        this.tempDiskUsage=null;
    }

    public SystemUsage(String name,PersistenceAdapter adapter,Store tempStore){
        this.parent=null;
        this.name=name;
        this.memoryUsage=new MemoryUsage(name+":memory");
        this.storeUsage=new StoreUsage(name+":store",adapter);
        this.tempDiskUsage=new TempUsage(name+":temp",tempStore);
    }

    public SystemUsage(SystemUsage parent,String name){
        this.parent=parent;
        this.name=name;
        this.memoryUsage=new MemoryUsage(parent.memoryUsage,name+":memory");
        this.storeUsage=new StoreUsage(parent.storeUsage,name+":store");
        this.tempDiskUsage=new TempUsage(parent!=null?parent.tempDiskUsage:null,name+":temp");
    }

    public String getName(){
        return name;
    }

    /**
     * @return the memoryUsage
     */
    public MemoryUsage getMemoryUsage(){
        return this.memoryUsage;
    }

    /**
     * @return the storeUsage
     */
    public StoreUsage getStoreUsage(){
        return this.storeUsage;
    }

    /**
     * @return the tempDiskUsage
     */
    public TempUsage getTempDiskUsage(){
        return this.tempDiskUsage;
    }

    public String toString(){
        return "UsageManager("+getName()+")";
    }

    public void start(){
        if(parent!=null){
            parent.addChild(this);
        }
        this.memoryUsage.start();
        this.storeUsage.start();
        this.tempDiskUsage.start();
    }

    public void stop(){
        if(parent!=null){
            parent.removeChild(this);
        }
        this.memoryUsage.stop();
        this.storeUsage.stop();
        this.tempDiskUsage.stop();
    }
    
    /**
     * Sets whether or not a send() should fail if there is no space free. The
     * default value is false which means to block the send() method until space
     * becomes available
     */
    public void setSendFailIfNoSpace(boolean failProducerIfNoSpace) {
        sendFailIfNoSpaceExplicitySet = true;
        this.sendFailIfNoSpace = failProducerIfNoSpace;
    }

    public boolean isSendFailIfNoSpace() {
        if (sendFailIfNoSpaceExplicitySet || parent == null) {
            return sendFailIfNoSpace;
        } else {
            return parent.isSendFailIfNoSpace();
        }
    }

    private void addChild(SystemUsage child){
        children.add(child);
    }

    private void removeChild(SystemUsage child){
        children.remove(child);
    }
}
