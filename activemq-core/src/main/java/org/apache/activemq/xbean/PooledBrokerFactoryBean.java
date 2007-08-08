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
package org.apache.activemq.xbean;

import java.util.HashMap;

import org.apache.activemq.broker.BrokerService;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

/**
 * Used to share a single broker even if you have multiple broker bean definitions.
 * 
 * A use case is where you have multiple web applications that want to start an embedded broker
 * but only the first one to deploy should actually start it.
 * 
 * @version $Revision$
 */
public class PooledBrokerFactoryBean implements FactoryBean, InitializingBean, DisposableBean {

    static final HashMap sharedBrokerMap = new HashMap();
    
    private boolean start;
    private Resource config;
    
    static class SharedBroker {
        BrokerFactoryBean factory;
        int refCount;
    }
    
    public void afterPropertiesSet() throws Exception {
        synchronized( sharedBrokerMap ) {
            SharedBroker sharedBroker = (SharedBroker) sharedBrokerMap.get(config.getFilename());
            if( sharedBroker == null ) {
                sharedBroker = new SharedBroker();
                sharedBroker.factory = new BrokerFactoryBean();
                sharedBroker.factory.setConfig(config);
                sharedBroker.factory.setStart(start);
                sharedBroker.factory.afterPropertiesSet();
                sharedBrokerMap.put(config.getFilename(), sharedBroker);
            }
            sharedBroker.refCount++;
        }
    }

    public void destroy() throws Exception {
        synchronized( sharedBrokerMap ) {
            SharedBroker sharedBroker = (SharedBroker) sharedBrokerMap.get(config.getFilename());
            if( sharedBroker != null ) {
                sharedBroker.refCount--;
                if( sharedBroker.refCount==0 ) {
                    sharedBroker.factory.destroy();
                    sharedBrokerMap.remove(config.getFilename());
                }
            }
        }
    }

    public Resource getConfig() {
        return config;
    }

    public Object getObject() throws Exception {
        synchronized( sharedBrokerMap ) {
            SharedBroker sharedBroker = (SharedBroker) sharedBrokerMap.get(config.getFilename());
            if( sharedBroker != null ) {
                return sharedBroker.factory.getObject();
            }
        }
        return null;
    }

    public Class getObjectType() {
        return BrokerService.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public boolean isStart() {
        return start;
    }

    public void setConfig(Resource config) {
        this.config = config;
    }

    public void setStart(boolean start) {
        this.start=start;
    }
    
}
