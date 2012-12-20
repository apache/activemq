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
package org.apache.activemq.broker.jmx;

import java.io.Serializable;

public class HealthStatus implements Serializable {
    private final String healthId;
    private final String level;
    private final String message;
    private final String resource;

    public HealthStatus(String healthId, String level, String message, String resource) {
        this.healthId = healthId;
        this.level = level;
        this.message = message;
        this.resource = resource;
    }

    public String getHealthId() {
        return healthId;
    }

    public String getLevel() {
        return level;
    }

    public String getMessage() {
        return message;
    }

    public String getResource() {
        return resource;
    }

    public String toString(){
        return healthId + ": " + level + " " + message + " from " + resource;
    }
}
