/** 
 * 
 * Copyright 2004 Hiram Chirino
 * Copyright 2005 LogicBlaze Inc.
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
 * 
 **/
package org.activemq.ra;

import javax.resource.spi.ConnectionRequestInfo;
import java.io.Serializable;


/**
 * @version $Revision$
 * 
 * Must override equals and hashCode (JCA spec 16.4)
 */
public class ActiveMQConnectionRequestInfo implements ConnectionRequestInfo, Serializable, Cloneable {

    private static final long serialVersionUID = -5754338187296859149L;

    private String userName;
    private String password;
    private String serverUrl;
    private String clientid;
    private Boolean useInboundSession;

    public ActiveMQConnectionRequestInfo copy() {
        try {
            return (ActiveMQConnectionRequestInfo) clone();
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException("Could not clone: ", e);
        }
    }


    /**
     * @see javax.resource.spi.ConnectionRequestInfo#hashCode()
     */
    public int hashCode() {
        int rc = 0;
        if (useInboundSession != null) {
            rc ^= useInboundSession.hashCode();
        }
        if (serverUrl != null) {
            rc ^= serverUrl.hashCode();
        }
        return rc;
    }


    /**
     * @see javax.resource.spi.ConnectionRequestInfo#equals(java.lang.Object)
     */
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!getClass().equals(o.getClass())) {
            return false;
        }
        ActiveMQConnectionRequestInfo i = (ActiveMQConnectionRequestInfo) o;
        if ( notEqual(serverUrl, i.serverUrl) ) {
            return false;
        }
        if ( notEqual(useInboundSession, i.useInboundSession) ) {
            return false;
        }
        return true;
    }


    /**
     * @param i
     * @return
     */
    private boolean notEqual(Object o1, Object o2) {
        return (o1 == null ^ o2 == null) || (o1 != null && !o1.equals(o2));
    }

    /**
     * @return Returns the url.
     */
    public String getServerUrl() {
        return serverUrl;
    }

    /**
     * @param url The url to set.
     */
    public void setServerUrl(String url) {
        this.serverUrl = url;
    }

    /**
     * @return Returns the password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password The password to set.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return Returns the userid.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * @param userid The userid to set.
     */
    public void setUserName(String userid) {
        this.userName = userid;
    }

    /**
     * @return Returns the clientid.
     */
    public String getClientid() {
        return clientid;
    }

    /**
     * @param clientid The clientid to set.
     */
    public void setClientid(String clientid) {
        this.clientid = clientid;
    }

    public String toString() {
        return "ActiveMQConnectionRequestInfo{ " +
                "userName = '" + userName + "' " +
                ", serverUrl = '" + serverUrl + "' " +
                ", clientid = '" + clientid + "' " +
                ", userName = '" + userName + "' " +
                ", useInboundSession = '" + useInboundSession + "' " +
                " }";
    }


    public Boolean getUseInboundSession() {
        return useInboundSession;
    }


    public void setUseInboundSession(Boolean useInboundSession) {
        this.useInboundSession = useInboundSession;
    }


    public boolean isUseInboundSessionEnabled() {
        return useInboundSession!=null && useInboundSession.booleanValue();
    }
}
