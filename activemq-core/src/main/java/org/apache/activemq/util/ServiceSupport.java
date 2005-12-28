/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
package org.apache.activemq.util;

import org.apache.activemq.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A helper class for working with services
 *  
 * @version $Revision: 1.1 $
 */
public abstract class ServiceSupport {
    private static final Log log = LogFactory.getLog(ServiceSupport.class);

    public static void dispose(Service service) {
        try {
            service.stop();
        }
        catch (Exception e) {
            log.error("Could not stop service: " + service + ". Reason: " + e, e);
        }
    }
    
    public void stop() throws Exception {
        ServiceStopper stopper = new ServiceStopper();
        stop(stopper);
        stopper.throwFirstException();
    }

    /**
     * Provides a way for derived classes to stop resources cleanly, handling exceptions
     */
    protected abstract void stop(ServiceStopper stopper);
}
