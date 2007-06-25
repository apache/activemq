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
package org.apache.activemq.console.filter;

public class WildcardToRegExTransformFilter extends WildcardTransformFilter {
    /**
     * Creates a filter that is able to transform a wildcard query to a regular expression query string
     * @param next - next query filter
     */
    public WildcardToRegExTransformFilter(RegExQueryFilter next) {
        super(next);
    }

    /**
     * Use to determine if a query string is a wildcard query. A query string is a wildcard query if it is a key-value
     * pair with the format <key>=<value> and the value contains '*' and '?'.
     * @param query - query string
     * @return true, if the query string is a wildcard query, false otherwise
     */
    protected boolean isWildcardQuery(String query) {
        // If the query is a key=value pair
        String key = query;
        String val = "";
        int pos = key.indexOf("=");
        if (pos >= 0) {
            val = key.substring(pos + 1);
        }

        // If the value contains wildcards
        return ((val.indexOf("*") >= 0) || (val.indexOf("?") >= 0));
    }

    /**
     * Transform a wildcard query to regular expression format
     * @param query - query string to transform
     * @return regex query string
     */
    protected String transformWildcardQuery(String query) {
        // Get the key=value pair
        String key = query;
        String val = "";
        int pos = key.indexOf("=");
        if (pos >= 0) {
            val = key.substring(pos + 1);
            key = key.substring(0, pos);
        }

        val = val.replaceAll("[.]", "\\\\."); // Escape all dot characters. From (.) to (\.)
        val = val.replaceAll("[?]", ".");     // Match single characters
        val = val.replaceAll("[*]", ".*?");   // Match all characters, use reluctant quantifier
        val = "(" + val +")";                 // Let's group the query for clarity
        val = RegExQueryFilter.REGEX_PREFIX + val; // Flag as a regular expression query

        return key + "=" + val;
    }
}
