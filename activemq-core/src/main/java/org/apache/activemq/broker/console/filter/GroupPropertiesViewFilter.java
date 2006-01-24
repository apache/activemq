/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.broker.console.filter;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

public class GroupPropertiesViewFilter extends PropertiesViewFilter {

    /**
     * Creates a group properties filter that is able to filter the display result based on a group prefix
     * @param next - the next query filter
     */
    public GroupPropertiesViewFilter(QueryFilter next) {
        super(next);
    }

    /**
     * Creates a group properties filter that is able to filter the display result based on a group prefix
     * @param groupView - the group filter to use
     * @param next - the next query filter
     */
    public GroupPropertiesViewFilter(Set groupView, QueryFilter next) {
        super(groupView, next);
    }

    /**
     * Filter the properties that matches the group prefix only.
     * @param data - map data to filter
     * @return - filtered map data
     */
    protected Map filterView(Map data) {
        // If no view specified, display all attributes
        if (viewFilter == null || viewFilter.isEmpty()) {
            return data;
        }

        Map newData;
        try {
            // Lets try to use the same class as the original
            newData = (Map)data.getClass().newInstance();
        } catch (Exception e) {
            // Lets use a default HashMap
            newData = new HashMap();
        }

        // Filter the keys to view
        for (Iterator i=data.keySet().iterator(); i.hasNext();) {
            String key = (String)i.next();

            // Checks if key matches any of the group filter
            for (Iterator j=viewFilter.iterator(); j.hasNext();) {
                String group = (String)j.next();
                if (key.startsWith(group)) {
                    newData.put(key, data.get(key));
                    break;
                }
            }
        }

        return newData;
    }
}
