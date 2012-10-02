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

public abstract class ResultTransformFilter implements QueryFilter {
    private QueryFilter next;

    /**
     * Constructs a query filter that transform the format of the query result
     * 
     * @param next - the query filter to retrieve the results from
     */
    protected ResultTransformFilter(QueryFilter next) {
        this.next = next;
    }

    /**
     * Transforms the queried results to a collection of different objects
     * 
     * @param query - the query string
     * @return collections of transformed objects
     * @throws Exception
     */
    public List query(String query) throws Exception {
        return transformList(next.query(query));
    }

    /**
     * Transforms the queried results to a collection of different objects
     * 
     * @param queries - the query map
     * @return collections of transformed objects
     * @throws Exception
     */
    public List<Object> query(List queries) throws Exception {
        return transformList(next.query(queries));
    }

    /**
     * Transforms a collection to a collection of different objects.
     * 
     * @param result - the collection to transform
     * @return collection of properties objects
     */
    protected List<Object> transformList(List<Object> result) throws Exception {
        List<Object> props = new ArrayList<Object>();

        for (Iterator<Object> i = result.iterator(); i.hasNext();) {
            props.add(transformElement(i.next()));
        }

        return props;
    }

    /**
     * Transform a result object
     * 
     * @param obj - the object instance to transform
     * @return the transformed object
     */
    protected abstract Object transformElement(Object obj) throws Exception;

}
