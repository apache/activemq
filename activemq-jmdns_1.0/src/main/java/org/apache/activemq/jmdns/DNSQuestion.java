/**
 * Copyright 2003-2005 Arthur van Hoff, Rick Blair
 *
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
package org.apache.activemq.jmdns;

import java.util.logging.Logger;

/**
 * A DNS question.
 *
 * @version %I%, %G%
 * @author	Arthur van Hoff
 */
final class DNSQuestion extends DNSEntry
{
    private static Logger logger = Logger.getLogger(DNSQuestion.class.toString());

    /**
     * Create a question.
     */
    DNSQuestion(String name, int type, int clazz)
    {
        super(name, type, clazz);
    }

    /**
     * Check if this question is answered by a given DNS record.
     */
    boolean answeredBy(DNSRecord rec)
    {
        return (clazz == rec.clazz) && ((type == rec.type) || (type == DNSConstants.TYPE_ANY)) &&
            name.equals(rec.name);
    }

    /**
     * For debugging only.
     */
    public String toString()
    {
        return toString("question", null);
    }
}
