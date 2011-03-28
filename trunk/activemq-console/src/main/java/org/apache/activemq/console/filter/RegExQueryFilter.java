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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class RegExQueryFilter extends AbstractQueryFilter {
    public static final String REGEX_PREFIX = "REGEX:QUERY:";

    /**
     * Creates a regular expression query that is able to match an object using
     * key-value pattern regex filtering
     * 
     * @param next
     */
    protected RegExQueryFilter(QueryFilter next) {
        super(next);
    }

    /**
     * Separates the regular expressions queries from the usual queries. A query
     * is a regex query, if it is key-value pair with the format <key>=<value>,
     * and value is a pattern that satisfies the isRegularExpression method.
     * 
     * @param queries - list of queries
     * @return filtered objects that matches the regex query
     * @throws Exception
     */
    public List query(List queries) throws Exception {
        Map regex = new HashMap();
        List newQueries = new ArrayList();

        // Lets parse for regular expression queries
        for (Iterator i = queries.iterator(); i.hasNext();) {
            // Get key-value pair
            String token = (String)i.next();
            String key = "";
            String val = "";
            int pos = token.indexOf("=");
            if (pos >= 0) {
                val = token.substring(pos + 1);
                key = token.substring(0, pos);
            }

            // Add the regex query to list and make it a non-factor in the
            // succeeding queries
            if (isRegularExpression(val)) {
                regex.put(key, compileQuery(val));

                // Add the normal query to the query list
            } else {
                newQueries.add(token);
            }
        }

        // Filter the result using the regular expressions specified
        return filterCollectionUsingRegEx(regex, next.query(newQueries));
    }

    /**
     * Checks if a given string is a regular expression query. Currently, a
     * pattern is a regex query, if it starts with the
     * RegExQueryFilter.REGEX_PREFIX.
     * 
     * @param query
     * @return
     */
    protected boolean isRegularExpression(String query) {
        return query.startsWith(REGEX_PREFIX);
    }

    /**
     * Compiles the regex query to a pattern.
     * 
     * @param query - query string to compile
     * @return regex pattern
     */
    protected Pattern compileQuery(String query) {
        return Pattern.compile(query.substring(REGEX_PREFIX.length()));
    }

    /**
     * Filter the specified colleciton using the regex patterns extracted.
     * 
     * @param regex - regex map
     * @param data - list of objects to filter
     * @return filtered list of objects that matches the regex map
     * @throws Exception
     */
    protected List filterCollectionUsingRegEx(Map regex, List data) throws Exception {
        // No regular expressions filtering needed
        if (regex == null || regex.isEmpty()) {
            return data;
        }

        List filteredElems = new ArrayList();

        // Get each data object to filter
        for (Iterator i = data.iterator(); i.hasNext();) {
            Object dataElem = i.next();
            // If properties of data matches all the regex pattern, add it
            if (matches(dataElem, regex)) {
                filteredElems.add(dataElem);
            }
        }

        return filteredElems;
    }

    /**
     * Determines how the object is to be matched to the regex map.
     * 
     * @param data - object to match
     * @param regex - regex map
     * @return true, if the object matches the regex map, false otherwise
     * @throws Exception
     */
    protected abstract boolean matches(Object data, Map regex) throws Exception;
}
