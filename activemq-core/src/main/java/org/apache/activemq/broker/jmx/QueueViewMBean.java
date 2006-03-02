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
package org.apache.activemq.broker.jmx;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;


public interface QueueViewMBean {
    
    public void gc();
    public void resetStatistics();

    public long getEnqueueCount();
    public long getDequeueCount();
    public long getConsumerCount();
    public long getMessages();
    public long getMessagesCached();

    
    public CompositeData[] browse() throws OpenDataException;
    public TabularData browseAsTable() throws OpenDataException;
    public CompositeData getMessage(String messageId) throws OpenDataException;
    public void removeMessage(String messageId);
    public void purge();

    public boolean copyMessageTo(String messageId, String destinationName) throws Throwable;
}