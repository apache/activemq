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
package org.apache.activemq.systest;

/**
 * A helper class used to stop a bunch of services, catching and logging any
 * exceptions and then throwing the first exception when everything is stoped.
 * 
 * @version $Revision: 1.1 $
 */
public class AgentStopper {
    private Exception firstException;

    /**
     * Stops the given service, catching any exceptions that are thrown.
     */
    public void stop(Agent service) {
        if (service != null) {
            try {
                service.stop(this);
            }
            catch (Exception e) {
                onException(service, e);
            }
        }
    }

    public void onException(Object owner, Exception e) {
        logError(owner, e);
        if (firstException == null) {
            firstException = e;
        }
    }

    /**
     * Throws the first exception that was thrown if there was one.
     */
    public void throwFirstException() throws Exception {
        if (firstException != null) {
            throw firstException;
        }
    }

    protected void logError(Object service, Exception e) {
        System.err.println("Could not stop service: " + service + ". Reason: " + e);
        e.printStackTrace(System.err);
    }

}
