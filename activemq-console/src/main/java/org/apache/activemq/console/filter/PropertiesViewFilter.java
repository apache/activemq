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
package org.apache.activemq.console.filter;

import java.util.*;

public class PropertiesViewFilter implements QueryFilter {
    protected QueryFilter next;
    protected Set viewFilter;

    /**
     * Creates a filter that will select the properties of a map object to view
     * 
     * @param next - the next query filter that will return a collection of maps
     */
    public PropertiesViewFilter(QueryFilter next) {
        this(null, next);
    }

    /**
     * Creates a filter that will select the properties of a map object to view
     * 
     * @param viewFilter - the properties to view
     * @param next - the next query filter that will return a collection of maps
     */
    public PropertiesViewFilter(Set viewFilter, QueryFilter next) {
        this.next = next;
        this.viewFilter = viewFilter;
    }

    /**
     * Filter the properties to view of the query result
     * 
     * @param query - the query string
     * @return list of objects that has been view filtered
     */
    public List<Map<Object, Object>> query(String query) throws Exception {
        return filterViewCollection(next.query(query), viewFilter);
    }

    /**
     * Filter the properties to view of the query result
     * 
     * @param queries - the query map
     * @return list of objects that has been view filtered
     * @throws Exception
     */
    public List<Map<Object, Object>> query(List queries) throws Exception {
        return filterViewCollection(next.query(queries), viewFilter);
    }

    /**
     * Filter the view of each element in the collection
     * 
     * @param result - the lists to filter the view from
     * @param viewFilter - the views to select
     * @return list of objects whose view has been filtered
     */
    protected List<Map<Object, Object>> filterViewCollection(Collection<Map<Object, Object>> result, Set viewFilter) {
        // Use a list to allow duplicate entries
        List<Map<Object, Object>> newCollection = new ArrayList<Map<Object, Object>>();

        for (Iterator<Map<Object, Object>> i = result.iterator(); i.hasNext();) {
            newCollection.add(filterView(i.next()));
        }

        return newCollection;
    }

    /**
     * Select only the attributes to view from the map data
     * 
     * @param data - data to filter the view from
     * @return - data with the view filtered
     */
    protected Map<Object, Object> filterView(Map<Object, Object> data) {
        // If no view specified, display all attributes
        if (viewFilter == null || viewFilter.isEmpty()) {
            return data;
        }

        Map<Object, Object> newData;
        try {
            // Lets try to use the same class as the original
            newData = new LinkedHashMap(data.getClass().newInstance());
        } catch (Exception e) {
            // Lets use a default HashMap
            newData = new LinkedHashMap<Object, Object>();
        }

        // Filter the keys to view
        for (Iterator i = viewFilter.iterator(); i.hasNext();) {
            Object key = i.next();
            Object val = data.get(key);

            if (val != null) {
                newData.put(key, val);
            }
        }

        return newData;
    }

}
