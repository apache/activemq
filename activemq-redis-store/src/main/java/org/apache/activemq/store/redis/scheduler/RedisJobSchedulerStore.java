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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;
import java.util.Date;

import org.apache.activemq.broker.scheduler.JobScheduler;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.redisson.Redisson;
import org.redisson.config.Config;
import org.redisson.api.RedissonClient;
import org.redisson.api.RBucket;
import org.redisson.api.RBuckets;

/**
 * A Redis JobSchedulerStore implementation used for Brokers that have persistence
 * disabled or when the JobSchedulerStore usage doesn't require a file or DB based store
 * implementation allowing for better performance.
 */
public class RedisJobSchedulerStore extends ServiceSupport implements JobSchedulerStore {

    private static final Logger LOG = LoggerFactory.getLogger(RedisJobSchedulerStore.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, RedisJobScheduler> schedulers = new HashMap<String, RedisJobScheduler>();

	private Config config = new Config();
	private RedissonClient redisson;
	private String keyPrefix = "amqSchedStore_";
	private RedisJobSchedulerStoreFilterFactory filterFactory = null;

	public void setRedisAddress(String address) {
        LOG.debug("Redis address: {}", address);
		// rediss - defines to use SSL for Redis connection
		// config.useSingleServer().setAddress("rediss://127.0.0.1:6379");
		config.useSingleServer().setAddress(address);
	}

	public void setRedisPassword(String password) {
        LOG.debug("Redis password set.");
		config.useSingleServer().setPassword(password);
	}

	public void setRedisKeyPrefix(String keyPrefix) {
		this.keyPrefix = keyPrefix;
        LOG.debug("Redis key prefix: {}", keyPrefix);
	}

	public void setSslEnableEndpointIdentification(boolean enableEndpointIdentification) {
		config.useSingleServer().setSslEnableEndpointIdentification(enableEndpointIdentification);
        LOG.debug("Redis enableEndpointIdentification: {}", enableEndpointIdentification);
	}

	public void setFilterFactory(RedisJobSchedulerStoreFilterFactory filterFactory) {
		this.filterFactory = filterFactory;
	}

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        for (RedisJobScheduler scheduler : schedulers.values()) {
            try {
                scheduler.stop();
            } catch (Exception e) {
                LOG.error("Failed to stop scheduler: {}", scheduler.getName(), e);
            }
        }
		redisson.shutdown();
    }

    @Override
    protected void doStart() throws Exception {
		redisson = Redisson.create(config);
        for (RedisJobScheduler scheduler : schedulers.values()) {
            try {
                scheduler.start();
            } catch (Exception e) {
                LOG.error("Failed to start scheduler: {}", scheduler.getName(), e);
            }
        }
    }

    @Override
    public JobScheduler getJobScheduler(String name) throws Exception {
        this.lock.lock();
        try {
            RedisJobScheduler result = this.schedulers.get(name);
            if (result == null) {
                LOG.debug("Creating new redis scheduler: {}", name);
                result = new RedisJobScheduler(name, this);
                this.schedulers.put(name, result);
                if (isStarted()) {
                    result.start();
                }
            }
            return result;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean removeJobScheduler(String name) throws Exception {
        boolean result = false;

        this.lock.lock();
        try {
            RedisJobScheduler scheduler = this.schedulers.remove(name);
            result = scheduler != null;
            if (result) {
                LOG.debug("Removing redis Job Scheduler: {}", name);
                scheduler.stop();
                this.schedulers.remove(name);
            }
        } finally {
            this.lock.unlock();
        }
        return result;
    }

    //---------- Methods that don't really apply to this implementation ------//

    @Override
    public long size() {
        return 0;
    }

    @Override
    public File getDirectory() {
        return null;
    }

    @Override
    public void setDirectory(File directory) {
    }

	public void storeJob(RedisJob job) {
		String key = keyPrefix+job.getJobId();
		Date now = new Date();
		Date exp = new Date(job.getNextTime());

		long millis = exp.getTime() - now.getTime();

		LOG.debug("Storing redis job: {} until {} for {} ms", key, exp, millis);

		RBucket<RedisJob> bucket = redisson.getBucket(key);
		bucket.set(job);
		if(!bucket.expireAt(exp.getTime())) {
			LOG.debug("ExpireAt redis job failed: {} at {}", key, exp);
			if(!bucket.expire(millis, TimeUnit.MILLISECONDS)) {
				LOG.debug("Expire redis job failed: {} for {} ms", key, millis);
			}
		}
	}

	public void removeJob(String jobId) {
		String key = keyPrefix+jobId;
		LOG.debug("Removing redis job: {} with key {}", jobId, key);
		RBucket<RedisJob> bucket = redisson.getBucket(key);
		bucket.delete();
	}

	public List<RedisJob> listJobs() throws Exception {
		String keyPattern = keyPrefix+"*";
		LOG.debug("Listing redis jobs for key {}", keyPattern);
		List<RedisJob> jobs = new ArrayList<RedisJob>();
		Iterable<String> keys = redisson.getKeys().getKeysByPattern(keyPattern);

		RedisJobSchedulerStoreFilter filter = null;
		if(null != filterFactory) {
			filter = filterFactory.getFilter();
		}

		for(String key : keys) {
			RBucket<RedisJob> bucket = redisson.getBucket(key);

			RedisJob job = null;

			if(null != filter) {
				job = filter.filter(bucket);
			} else {
				job = bucket.get();
			}

			if(null == job) {
				LOG.debug("Redis job {} filtered out", job.getJobId());
				continue;
			}

			LOG.debug("Redis job {}", job.getJobId());
			jobs.add(job);
		}
		return jobs;
	}
}
