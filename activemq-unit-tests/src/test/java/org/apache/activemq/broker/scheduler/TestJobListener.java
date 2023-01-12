/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.scheduler;

import java.util.concurrent.CountDownLatch;

import org.apache.activemq.util.ByteSequence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJobListener implements JobListener {

	private static final Logger LOG = LoggerFactory.getLogger(TestJobListener.class);

	private int count;
	private CountDownLatch latch;

	public TestJobListener(int count) {
		this.count = count;
		this.latch = new CountDownLatch(count);
	}

	public TestJobListener(CountDownLatch latch) {
		this.count = (int)latch.getCount();
		this.latch = latch;
	}

	public int getCount() {
		return count;
	}

	public CountDownLatch getLatch() {
		return latch;
	}

	@Override
	public void willScheduleJob(String id, ByteSequence job) throws Exception {
		LOG.info("willScheduleJob({}): {}", count - latch.getCount() + 1, id);
	}

	@Override
	public void scheduleJob(String id, ByteSequence job) throws Exception {
		LOG.info("scheduleJob({}): {}", count - latch.getCount() + 1, id);
	}

	@Override
	public void didScheduleJob(String id, ByteSequence job) throws Exception {
		LOG.info("didScheduleJob({}): {}", count - latch.getCount() + 1, id);
	}

	@Override
	public void willDispatchJob(String id, ByteSequence job) throws Exception {
		LOG.info("willDispatchJob({}): {}", count - latch.getCount() + 1, id);
	}

	@Override
	public void dispatchJob(String id, ByteSequence job) throws Exception {
		LOG.info("dispatchJob({}): {}", count - latch.getCount() + 1, id);
	}

	// Count down at the end of the job lifecycle
	@Override
	public void didDispatchJob(String id, ByteSequence job) throws Exception {
		LOG.info("didDispatchJob({}): {}", count - latch.getCount() + 1, id);
		latch.countDown();
	}
}
