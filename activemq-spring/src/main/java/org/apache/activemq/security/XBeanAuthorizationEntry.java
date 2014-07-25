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

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.InitializingBean;

/**
 * Represents an entry in a {@link DefaultAuthorizationMap} for assigning
 * different operations (read, write, admin) of user roles to a specific
 * destination or a hierarchical wildcard area of destinations.
 *
 * @org.apache.xbean.XBean element="authorizationEntry"
 *
 */
public class XBeanAuthorizationEntry extends AuthorizationEntry implements InitializingBean {

    @Override
    public void setAdmin(String roles) throws Exception {
        adminRoles = roles;
    }

    @Override
    public void setRead(String roles) throws Exception {
        readRoles = roles;
    }

    @Override
    public void setWrite(String roles) throws Exception {
        writeRoles = roles;
    }

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
     * @org.apache.xbean.InitMethod
     */
    @Override
    public void afterPropertiesSet() throws Exception {

        if (adminRoles != null) {
            setAdminACLs(parseACLs(adminRoles));
        }

        if (writeRoles != null) {
            setWriteACLs(parseACLs(writeRoles));
        }

        if (readRoles != null) {
            setReadACLs(parseACLs(readRoles));
        }
    }

    @Override
    public String toString() {
        return "XBeanAuthEntry:" + adminRoles + "," + writeRoles + "," + readRoles;
    }
}
