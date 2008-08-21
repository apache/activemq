/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.jmx;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @version $Revision: 1.1 $
 */
public class CompositeDataHelper {

    /**
     * Extracts the named TabularData field from the CompositeData and converts it to a Map
     * which is the method used to get the typesafe user properties.
     */
    public static Map getTabularMap(CompositeData cdata, String fieldName) {
        Map map = new HashMap();
        Object tabularObject = cdata.get(fieldName);
        if (tabularObject instanceof TabularData) {
            TabularData tabularData = (TabularData) tabularObject;
            Collection<CompositeData> values = tabularData.values();
            for (CompositeData compositeData : values) {
                Object key = compositeData.get("key");
                Object value = compositeData.get("value");
                map.put(key, value);
            }
        }
        return map;
    }
}
