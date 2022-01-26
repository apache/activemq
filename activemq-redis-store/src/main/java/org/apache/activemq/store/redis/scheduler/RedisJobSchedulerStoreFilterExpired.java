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

package org.apache.activemq.store.redis.scheduler;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.redisson.api.RBucket;

/**
 * A Redis JobSchedulerStore implementation used for Brokers that have persistence
 * disabled or when the JobSchedulerStore usage doesn't require a file or DB based store
 * implementation allowing for better performance.
 */
public class RedisJobSchedulerStoreFilterExpired implements RedisJobSchedulerStoreFilter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisJobSchedulerStoreFilterExpired.class);

	private Date now = new Date();
	private long nowTime = now.getTime();

	public RedisJob filter(RBucket<RedisJob> bucket) {
		RedisJob job = bucket.get();

		long jobTime = job.getNextTime();
		Date jobDate = new Date(jobTime);

		if(jobTime < nowTime) {
			long millis = nowTime - jobTime;
			LOG.debug("Redis job {} expired {} millis ago at {}", job.getJobId(), millis, jobDate);
			bucket.delete();
			return null;
		}

		return job;
	}
}

