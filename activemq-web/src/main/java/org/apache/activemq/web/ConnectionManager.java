/**
 *
 * Copyright 2004 The Apache Software Foundation
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
 */

package org.apache.activemq.web;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

/**
 * Listens to sessions closing to ensure that JMS connections are
 * cleaned up nicely
 *
 * @version $Revision: 1.1.1.1 $
 */
public class ConnectionManager implements HttpSessionListener {
    private static final Log log = LogFactory.getLog(ConnectionManager.class);

    public void sessionCreated(HttpSessionEvent event) {
    }

    public void sessionDestroyed(HttpSessionEvent event) {
        /** TODO we can't use the session any more now!
         WebClient client = WebClient.getWebClient(event.getSession());
         try {
         client.stop();
         }
         catch (JMSException e) {
         log.warn("Error closing connection: " + e, e);
         }
         */
    }
}
