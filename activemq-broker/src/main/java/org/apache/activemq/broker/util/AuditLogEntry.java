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

package org.apache.activemq.broker.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.broker.jmx.Sensitive;

public class AuditLogEntry {

    protected String user = "anonymous";
    protected long timestamp;
    protected String operation;
    protected String remoteAddr;

    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss,SSS");

    protected Map<String, Object> parameters = new HashMap<String, Object>();

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getFormattedTime() {
        return formatter.format(new Date(timestamp));
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

   /**
    * Method to remove any sensitive parameters before logging.  Replaces any sensitive value with ****.  Sensitive
    * values are defined on MBean interface implementation method parameters using the @Sensitive annotation.
    *
    * @param arguments A array of arguments to test against method signature
    * @param method The method to test the arguments against.
    */
    public static Object[] sanitizeArguments(Object[] arguments, Method method)
    {
       Object[] sanitizedArguments = arguments.clone();
       Annotation[][] parameterAnnotations = method.getParameterAnnotations();

       for (int i = 0; i < arguments.length; i++)
       {
          for (Annotation annotation : parameterAnnotations[i])
          {
             if (annotation instanceof Sensitive)
             {
                sanitizedArguments[i] = "****";
                break;
             }
          }
       }
       return sanitizedArguments;
    }
}
