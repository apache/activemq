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

// REMIND: Listener should follow Java idiom for listener or have a different
//         name.

/**
 * DNSListener.
 * Listener for record updates.
 *
 * @author Werner Randelshofer, Rick Blair
 * @version 1.0  May 22, 2004  Created.
 */
interface DNSListener
{
    /**
     * Update a DNS record.
     */
    void updateRecord(JmDNS jmdns, long now, DNSRecord record);
}
