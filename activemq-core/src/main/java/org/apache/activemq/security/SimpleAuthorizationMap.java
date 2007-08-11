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

import java.util.Set;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.DestinationMap;

/**
 * An AuthorizationMap which is configured with individual DestinationMaps for
 * each operation.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class SimpleAuthorizationMap implements AuthorizationMap {

    private DestinationMap writeACLs;
    private DestinationMap readACLs;
    private DestinationMap adminACLs;

    private TempDestinationAuthorizationEntry tempDestinationAuthorizationEntry;

    public SimpleAuthorizationMap() {
    }

    public SimpleAuthorizationMap(DestinationMap writeACLs, DestinationMap readACLs, DestinationMap adminACLs) {
        this.writeACLs = writeACLs;
        this.readACLs = readACLs;
        this.adminACLs = adminACLs;
    }

    /*
     * Need to think how to retrieve the ACLs for temporary destinations since
     * they are not map to a specific destination. For now we'll just retrieve
     * it from a TempDestinationAuthorizationEntry same way as the
     * DefaultAuthorizationMap. The ACLs retrieved here will be map to all temp
     * destinations
     */

    public void setTempDestinationAuthorizationEntry(
                                                     TempDestinationAuthorizationEntry tempDestinationAuthorizationEntry) {
        this.tempDestinationAuthorizationEntry = tempDestinationAuthorizationEntry;
    }

    public TempDestinationAuthorizationEntry getTempDestinationAuthorizationEntry() {
        return this.tempDestinationAuthorizationEntry;
    }

    public Set<Object> getTempDestinationAdminACLs() {
        if (tempDestinationAuthorizationEntry != null) {
            return tempDestinationAuthorizationEntry.getAdminACLs();
        } else {
            return null;
        }
    }

    public Set<Object> getTempDestinationReadACLs() {
        if (tempDestinationAuthorizationEntry != null) {
            return tempDestinationAuthorizationEntry.getReadACLs();
        } else {
            return null;
        }
    }

    public Set<Object> getTempDestinationWriteACLs() {
        if (tempDestinationAuthorizationEntry != null) {
            return tempDestinationAuthorizationEntry.getWriteACLs();
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public Set<Object> getAdminACLs(ActiveMQDestination destination) {
        return adminACLs.get(destination);
    }

    @SuppressWarnings("unchecked")
    public Set<Object> getReadACLs(ActiveMQDestination destination) {
        return readACLs.get(destination);
    }

    @SuppressWarnings("unchecked")
    public Set<Object> getWriteACLs(ActiveMQDestination destination) {
        return writeACLs.get(destination);
    }

    public DestinationMap getAdminACLs() {
        return adminACLs;
    }

    public void setAdminACLs(DestinationMap adminACLs) {
        this.adminACLs = adminACLs;
    }

    public DestinationMap getReadACLs() {
        return readACLs;
    }

    public void setReadACLs(DestinationMap readACLs) {
        this.readACLs = readACLs;
    }

    public DestinationMap getWriteACLs() {
        return writeACLs;
    }

    public void setWriteACLs(DestinationMap writeACLs) {
        this.writeACLs = writeACLs;
    }

}
