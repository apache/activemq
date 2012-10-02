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
package org.apache.activemq.tool.properties;

public class JmsClientProperties extends AbstractObjectProperties {
    public static final String SESSION_AUTO_ACKNOWLEDGE = "autoAck";
    public static final String SESSION_CLIENT_ACKNOWLEDGE = "clientAck";
    public static final String SESSION_DUPS_OK_ACKNOWLEDGE = "dupsAck";
    public static final String SESSION_TRANSACTED = "transacted";

    protected String destName = "TEST.FOO";
    protected boolean destComposite;

    protected String sessAckMode = SESSION_AUTO_ACKNOWLEDGE;
    protected boolean sessTransacted;
    
    // commit transaction after X msgs only. 
    protected int commitAfterXMsgs = 1;

    protected String jmsProvider;
    protected String jmsVersion;
    protected String jmsProperties;

    public String getDestName() {
        return destName;
    }

    public void setDestName(String destName) {
        this.destName = destName;
    }

    public boolean isDestComposite() {
        return destComposite;
    }

    public void setDestComposite(boolean destComposite) {
        this.destComposite = destComposite;
    }

    public String getSessAckMode() {
        return sessAckMode;
    }

    public void setSessAckMode(String sessAckMode) {
        this.sessAckMode = sessAckMode;
    }

    public boolean isSessTransacted() {
        return sessTransacted;
    }

    public void setSessTransacted(boolean sessTransacted) {
        this.sessTransacted = sessTransacted;
    }
    
    public void setCommitAfterXMsgs(int commitAfterXMsg) {
    	this.commitAfterXMsgs = commitAfterXMsg;
    }
    
    public int getCommitAfterXMsgs() {
    	return this.commitAfterXMsgs;
    }

    public String getJmsProvider() {
        return jmsProvider;
    }

    public void setJmsProvider(String jmsProvider) {
        this.jmsProvider = jmsProvider;
    }

    public String getJmsVersion() {
        return jmsVersion;
    }

    public void setJmsVersion(String jmsVersion) {
        this.jmsVersion = jmsVersion;
    }

    public String getJmsProperties() {
        return jmsProperties;
    }

    public void setJmsProperties(String jmsProperties) {
        this.jmsProperties = jmsProperties;
    }
}
