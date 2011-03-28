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

import org.slf4j.MDC;

import java.util.Hashtable;
import java.util.Map;

/**
 *  Helper class as MDC Log4J adapter doesn't behave well with null values
 */
public class MDCHelper {

    public static Map getCopyOfContextMap() {
        Map map = MDC.getCopyOfContextMap();
        if (map == null) {
            map = new Hashtable();
        }
        return map;
    }

    public static void setContextMap(Map map) {
        if (map == null) {
            map = new Hashtable();
        }
        MDC.setContextMap(map);
    }

}
