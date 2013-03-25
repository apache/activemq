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
package org.apache.activemq.security;

import org.apache.activemq.filter.DestinationMapEntry;
import org.springframework.beans.factory.InitializingBean;

import javax.annotation.PostConstruct;
import java.util.List;


/**
 *  @org.apache.xbean.XBean element="authorizationMap"
 */
public class XBeanAuthorizationMap extends DefaultAuthorizationMap implements InitializingBean {

    protected List<DestinationMapEntry> authorizationEntries;

    /**
     *
     * @org.apache.xbean.InitMethod
     */
    @PostConstruct
    public void afterPropertiesSet() throws Exception {
        for (DestinationMapEntry entry : authorizationEntries) {
            if (((XBeanAuthorizationEntry)entry).getGroupClass() == null) {
                ((XBeanAuthorizationEntry)entry).setGroupClass(groupClass);
            }
            ((XBeanAuthorizationEntry)entry).afterPropertiesSet();
        }
        super.setEntries(authorizationEntries);
    }

    /**
     * Sets the individual entries on the authorization map
     *
     * @org.apache.xbean.ElementType class="org.apache.activemq.security.AuthorizationEntry"
     */
    @SuppressWarnings("rawtypes")
    public void setAuthorizationEntries(List<DestinationMapEntry> entries) {
        this.authorizationEntries = entries;
    }

}
