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
package org.apache.activemq.web;

import java.util.HashMap;
import java.util.Map;
import java.util.Date;

import javax.jms.MessageConsumer;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.activemq.MessageAvailableConsumer;

/*
 * Collection of all data needed to fulfill requests from a single web client.
 */
public class AjaxWebClient extends WebClient {
    private static final Logger LOG = LoggerFactory.getLogger(AjaxWebClient.class);
    
    // an instance which has not been accessed in this many milliseconds can be removed.
    final long expireAfter = 60 * 1000;
    
    Map<MessageAvailableConsumer, String> idMap;
    Map<MessageAvailableConsumer, String> destinationNameMap;
    AjaxListener listener;
    Long lastAccessed;
    
    public AjaxWebClient( HttpServletRequest request, long maximumReadTimeout ) {
        // 'id' meaning the first argument to the JavaScript addListener() function.
        // used to indicate which JS callback should handle a given message.
        this.idMap = new HashMap<MessageAvailableConsumer, String>();
        
        // map consumers to destinations like topic://test, etc.
        this.destinationNameMap = new HashMap<MessageAvailableConsumer, String>();
        
        this.listener = new AjaxListener( this, maximumReadTimeout );
        
        this.lastAccessed = this.getNow();
    }
    
    public Map<MessageAvailableConsumer, String> getIdMap() {
        return this.idMap;
    }
    
    public Map<MessageAvailableConsumer, String> getDestinationNameMap() {
        return this.destinationNameMap;
    }
    
    public AjaxListener getListener() {
        return this.listener;
    }
    
    public long getMillisSinceLastAccessed() {
        return this.getNow() - this.lastAccessed;
    }
    
    public void updateLastAccessed() {
        this.lastAccessed = this.getNow();
    }
    
    public boolean closeIfExpired() {
        long now = (new Date()).getTime();
        boolean returnVal = false;
        if( this.getMillisSinceLastAccessed() > this.expireAfter ) {
            this.close();
            returnVal = true;
        }
        return returnVal;
    }
    
    protected long getNow() {
        return (new Date()).getTime();
    }
}
