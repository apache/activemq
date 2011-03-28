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

import java.util.List;

public class StubQueryFilter implements QueryFilter {
    private List data;

    /**
     * Creates a stub query that returns the given collections as the query
     * result
     * 
     * @param data - the stub query result
     */
    public StubQueryFilter(List data) {
        this.data = data;
    }

    /**
     * Returns the provided stub data as a stub query result
     * 
     * @param queryStr - not use
     * @return the stub query result
     * @throws Exception
     */
    public List query(String queryStr) throws Exception {
        return data;
    }

    /**
     * Returns the provided stub data as a stub query result
     * 
     * @param queries - not use
     * @return the stub query result
     * @throws Exception
     */
    public List query(List queries) throws Exception {
        return data;
    }
}
