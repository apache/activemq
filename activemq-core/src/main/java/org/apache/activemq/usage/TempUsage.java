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

import org.apache.activemq.kaha.Store;

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
public class TempUsage extends Usage{

    final private Store store;

        
    public TempUsage(String name,Store store){
        super(null,name,1.0f);
        this.store=store;
    }
    
    public TempUsage(TempUsage parent,String name){
        super(parent,name,1.0f);
        this.store=parent.store;
    }

    protected long retrieveUsage(){
        return store.size();
    }
}