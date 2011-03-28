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
package org.apache.activemq.util;

import java.util.Iterator;
import java.util.List;

import org.apache.activemq.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class used to stop a bunch of services, catching and logging any
 * exceptions and then throwing the first exception when everything is stoped.
 * 
 * 
 */
public class ServiceStopper {
    private Throwable firstException;

    /**
     * Stops the given service, catching any exceptions that are thrown.
     */
    public void stop(Service service) {
        try {
            if (service != null) {
                service.stop();
            }
        } catch (Exception e) {
            onException(service, e);
        }
    }

    /**
     * Performs the given code to stop some service handling the exceptions
     * which may be thrown properly
     */
    public void run(Callback stopClosure) {
        try {
            stopClosure.execute();
        } catch (Throwable e) {
            onException(stopClosure, e);
        }
    }

    /**
     * Stops a list of services
     */
    public void stopServices(List services) {
        for (Iterator iter = services.iterator(); iter.hasNext();) {
            Service service = (Service)iter.next();
            stop(service);
        }
    }

    public void onException(Object owner, Throwable e) {
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
            if (firstException instanceof Exception) {
                Exception e = (Exception)firstException;
                throw e;
            } else if (firstException instanceof RuntimeException) {
                RuntimeException e = (RuntimeException)firstException;
                throw e;
            } else {
                throw new RuntimeException("Unknown type of exception: " + firstException, firstException);
            }
        }
    }

    protected void logError(Object service, Throwable e) {
        Logger log = LoggerFactory.getLogger(service.getClass());
        log.error("Could not stop service: " + service + ". Reason: " + e, e);
    }

}
