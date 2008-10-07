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
package org.apache.activemq.web.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.web.BrokerFacade;
import org.apache.activemq.web.DestinationFacade;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.Controller;

/**
 * Copies a message from one to another queue
 * 
 * @author <a href="http://www.nighttale.net">Dejan Bosanac</a>
 * @version $Revision$
 */
public class CopyMessage extends DestinationFacade implements Controller {
    private String messageId;
    private String destination;
    private static final Log log = LogFactory.getLog(CopyMessage.class);

    public CopyMessage(BrokerFacade brokerFacade) {
        super(brokerFacade);
    }

    public ModelAndView handleRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
        if (messageId != null) {
            QueueViewMBean queueView = getQueueView();
            if (queueView != null) {
            	log.info(getJMSDestination() + "(" + messageId + ")" + " copy to " + destination);
                queueView.copyMessageTo(messageId, destination);
            } else {
            	log.warn("No queue named: " + getPhysicalDestinationName());
            }
        }
        return redirectToBrowseView();
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
    
    

    public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

}
