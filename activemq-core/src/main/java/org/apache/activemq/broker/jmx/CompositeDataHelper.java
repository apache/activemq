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
        appendTabularMap(map, cdata, fieldName);
        return map;
    }

    public static void appendTabularMap(Map map, CompositeData cdata, String fieldName) {
        Object tabularObject = cdata.get(fieldName);
        if (tabularObject instanceof TabularData) {
            TabularData tabularData = (TabularData) tabularObject;
            Collection<CompositeData> values = (Collection<CompositeData>) tabularData.values();
            for (CompositeData compositeData : values) {
                Object key = compositeData.get("key");
                Object value = compositeData.get("value");
                map.put(key, value);
            }
        }
    }

    /**
     * Returns a map of all the user properties in the given message {@link javax.management.openmbean.CompositeData}
     * object
     *
     * @param cdata
     * @return
     */
    public static Map getMessageUserProperties(CompositeData cdata) {
        Map map = new HashMap();
        appendTabularMap(map, cdata, CompositeDataConstants.STRING_PROPERTIES);
        appendTabularMap(map, cdata, CompositeDataConstants.BOOLEAN_PROPERTIES);
        appendTabularMap(map, cdata, CompositeDataConstants.BYTE_PROPERTIES);
        appendTabularMap(map, cdata, CompositeDataConstants.SHORT_PROPERTIES);
        appendTabularMap(map, cdata, CompositeDataConstants.INT_PROPERTIES);
        appendTabularMap(map, cdata, CompositeDataConstants.LONG_PROPERTIES);
        appendTabularMap(map, cdata, CompositeDataConstants.FLOAT_PROPERTIES);
        appendTabularMap(map, cdata, CompositeDataConstants.DOUBLE_PROPERTIES);
        return map;
    }
}
