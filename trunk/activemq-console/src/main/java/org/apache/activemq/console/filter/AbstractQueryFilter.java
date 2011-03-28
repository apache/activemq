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

import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

public abstract class AbstractQueryFilter implements QueryFilter {
    protected QueryFilter next;

    /**
     * Creates a query filter, with the next filter specified by next.
     * @param next - the next query filter
     */
    protected AbstractQueryFilter(QueryFilter next) {
        this.next = next;
    }

    /**
     * Performs a query given the query string
     * @param query - query string
     * @return objects that matches the query
     * @throws Exception
     */
    public List query(String query) throws Exception {
        // Converts string query to map query
        StringTokenizer tokens = new StringTokenizer(query, QUERY_DELIMETER);
        return query(Collections.list(tokens));
    }

}
