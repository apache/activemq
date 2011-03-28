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
import java.util.Iterator;
import java.util.List;

public abstract class WildcardTransformFilter extends AbstractQueryFilter {

    /**
     * Creates a wildcard transform filter that is able to convert a wildcard
     * expression (determined by isWildcardQuery) to a another query type (use
     * transformWildcardQuery).
     * 
     * @param next - the next query filter
     */
    protected WildcardTransformFilter(QueryFilter next) {
        super(next);
    }

    /**
     * Converts the query list to set of different queries
     * 
     * @param queries - query list to transform
     * @return - result of the query
     * @throws Exception
     */
    public List query(List queries) throws Exception {
        List newQueries = new ArrayList();

        for (Iterator i = queries.iterator(); i.hasNext();) {
            String queryToken = (String)i.next();

            // Transform the wildcard query
            if (isWildcardQuery(queryToken)) {
                // Transform the value part only
                newQueries.add(transformWildcardQuery(queryToken));

                // Maintain the query as is
            } else {
                newQueries.add(queryToken);
            }
        }

        return next.query(newQueries);
    }

    /**
     * Use to determine is a query string is a wildcard query
     * 
     * @param query - query string
     * @return true, if the query string is a wildcard query, false otherwise
     */
    protected abstract boolean isWildcardQuery(String query);

    /**
     * Use to transform a wildcard query string to another query format
     * 
     * @param query - query string to transform
     * @return transformed query
     */
    protected abstract String transformWildcardQuery(String query);
}
