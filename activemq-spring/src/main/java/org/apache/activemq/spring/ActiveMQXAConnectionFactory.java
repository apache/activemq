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
package org.apache.activemq.spring;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.BeanNameAware;

/**
 * A <a href="http://www.springframework.org/">Spring</a> enhanced XA connection
 * factory which will automatically use the Spring bean name as the clientIDPrefix property
 * so that connections created have client IDs related to your Spring.xml file for
 * easier comprehension from <a href="http://activemq.apache.org/jmx.html">JMX</a>.
 *
 * @org.apache.xbean.XBean element="xaConnectionFactory"
 */
public class ActiveMQXAConnectionFactory extends org.apache.activemq.ActiveMQXAConnectionFactory implements BeanNameAware {

    private String beanName;
    private boolean useBeanNameAsClientIdPrefix;

    /**
     * JSR-250 callback wrapper; converts checked exceptions to runtime exceptions
     *
     * delegates to afterPropertiesSet, done to prevent backwards incompatible signature change.
     */
    @PostConstruct
    private void postConstruct() {
        try {
            afterPropertiesSet();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     *
     * @throws Exception
     * @org.apache.xbean.InitMethod
     */
    public void afterPropertiesSet() throws Exception {
        if (isUseBeanNameAsClientIdPrefix() && getClientIDPrefix() == null) {
            setClientIDPrefix(getBeanName());
        }
    }

    public String getBeanName() {
        return beanName;
    }

    @Override
    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    public boolean isUseBeanNameAsClientIdPrefix() {
        return useBeanNameAsClientIdPrefix;
    }

    public void setUseBeanNameAsClientIdPrefix(boolean useBeanNameAsClientIdPrefix) {
        this.useBeanNameAsClientIdPrefix = useBeanNameAsClientIdPrefix;
    }
}
