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


/**
 * Used to keep track of how much of something is being used so that a
 * productive working set usage can be controlled.
 * 
 * Main use case is manage memory usage.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: 1.3 $
 */
public class MemoryUsage extends Usage{

    private MemoryUsage parent;
    private long usage;

    public MemoryUsage(){
        this(null,"default");
    }

    /**
     * Create the memory manager linked to a parent. When the memory manager is
     * linked to a parent then when usage increased or decreased, the parent's
     * usage is also increased or decreased.
     * 
     * @param parent
     */
    public MemoryUsage(MemoryUsage parent){
        this(parent,"default");
    }

    public MemoryUsage(String name){
        this(null,name);
    }

    public MemoryUsage(MemoryUsage parent,String name){
        this(parent,name,1.0f);
    }

    public MemoryUsage(MemoryUsage parent,String name,float portion){
        super(parent,name,portion);
    }
    
    /**
     * @throws InterruptedException
     */
    public void waitForSpace() throws InterruptedException{
        if(parent!=null){
            parent.waitForSpace();
        }
        synchronized(usageMutex){
            for(int i=0;percentUsage>=100;i++){
                usageMutex.wait();
            }
        }
    }

    /**
     * @param timeout 
     * @throws InterruptedException
     * 
     * @return true if space
     */
    public boolean waitForSpace(long timeout) throws InterruptedException{
        if(parent!=null){
            if(!parent.waitForSpace(timeout)){
                return false;
            }
        }
        synchronized(usageMutex){
            if(percentUsage>=100){
                usageMutex.wait(timeout);
            }
            return percentUsage<100;
        }
    }
    
    public boolean isFull(){
        if(parent!=null&&parent.isFull()){
            return true;
        }
        synchronized(usageMutex){
            return percentUsage>=100;
        }
    }

    /**
     * Tries to increase the usage by value amount but blocks if this object is
     * currently full.
     * @param value 
     * 
     * @throws InterruptedException
     */
    public void enqueueUsage(long value) throws InterruptedException{
        waitForSpace();
        increaseUsage(value);
    }

    /**
     * Increases the usage by the value amount.
     * 
     * @param value
     */
    public void increaseUsage(long value){
        if(value==0){
            return;
        }
        if(parent!=null){
            parent.increaseUsage(value);
        }
        int percentUsage;
        synchronized(usageMutex){
            usage+=value;
            percentUsage=caclPercentUsage();
        }
        setPercentUsage(percentUsage);
    }

    /**
     * Decreases the usage by the value amount.
     * 
     * @param value
     */
    public void decreaseUsage(long value){
        if(value==0){
            return;
        }
        if(parent!=null){
            parent.decreaseUsage(value);
        }
        int percentUsage;
        synchronized(usageMutex){
            usage-=value;
            percentUsage=caclPercentUsage();
        }
        setPercentUsage(percentUsage);
    }

    protected long retrieveUsage(){
        return usage;
    }
}
