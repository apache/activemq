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
package org.apache.activemq.broker.region.group;

/**
 * A factory to create instances of {@link SimpleMessageGroupMap} when
 * implementing the <a
 * href="http://activemq.apache.org/message-groups.html">Message
 * Groups</a> functionality.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class MessageGroupHashBucketFactory implements MessageGroupMapFactory {

    private int bucketCount = 1024;

    public MessageGroupMap createMessageGroupMap() {
        return new MessageGroupHashBucket(bucketCount);
    }

    public int getBucketCount() {
        return bucketCount;
    }

    /**
     * Sets the number of hash buckets to use for the message group
     * functionality. This is only applicable to using message groups to
     * parallelize processing of a queue while preserving order across an
     * individual JMSXGroupID header value. This value sets the number of hash
     * buckets that will be used (i.e. the maximum possible concurrency).
     */
    public void setBucketCount(int bucketCount) {
        this.bucketCount = bucketCount;
    }

}
