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

package org.apache.activemq.tool.sampler;

import org.apache.activemq.tool.ClientRunBasis;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

public class AbstractPerformanceSamplerTest {

    private class EmptySampler extends AbstractPerformanceSampler {
        @Override
        public void sampleData() {}
    }

    private AbstractPerformanceSampler sampler;
    private CountDownLatch samplerLatch;

    @Before
    public void setUpSampler() {
        sampler = new EmptySampler();
        samplerLatch = new CountDownLatch(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetRampUpPercent_exceeds100() {
        sampler.setRampUpPercent(101);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetRampUpPercent_lessThan0() {
        sampler.setRampUpPercent(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetRampDownPercent_exceeds99() {
        sampler.setRampDownPercent(100);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetRampDownPercent_lessThan0() {
        sampler.setRampDownPercent(-1);
    }

    @Test
    public void testSamplerOnCountBasis() throws InterruptedException {
        final CountDownLatch latch = samplerLatch;
        sampler.startSampler(latch, ClientRunBasis.count, 0);
        sampler.finishSampling();
        samplerLatch.await();
        assertNull(sampler.getDuration());
        assertEquals(0, (long) sampler.getRampUpTime());
        assertEquals(0, (long) sampler.getRampDownTime());
    }

    @Test
    public void testSamplerOnTimeBasis_matchesClientSettings() throws InterruptedException {
        final CountDownLatch latch = samplerLatch;
        sampler.startSampler(latch, ClientRunBasis.time, 1000);
        samplerLatch.await();
        assertEquals(1000, (long) sampler.getDuration());
        assertEquals(0, (long) sampler.getRampUpTime());
        assertEquals(0, (long) sampler.getRampDownTime());
    }

    @Test
    public void testSamplerOnTimeBasis_percentageOverrides() throws InterruptedException {
        final CountDownLatch latch = samplerLatch;
        sampler.setRampUpPercent(10);
        sampler.setRampDownPercent(20);
        sampler.startSampler(latch, ClientRunBasis.time, 1000);
        samplerLatch.await();
        assertEquals(1000, (long) sampler.getDuration());
        assertEquals(100, (long) sampler.getRampUpTime());
        assertEquals(200, (long) sampler.getRampDownTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSamplerOnTimeBasis_percentageOverridesExceedSamplerDuration() throws InterruptedException {
        final CountDownLatch latch = samplerLatch;
        sampler.setRampUpPercent(60);
        sampler.setRampDownPercent(41);
        sampler.startSampler(latch, ClientRunBasis.time, 1000);
    }

    @Test
    public void testSamplerOnTimeBasis_timeOverrides() throws InterruptedException {
        final CountDownLatch latch = samplerLatch;
        sampler.setRampUpTime(10);
        sampler.setRampDownTime(20);
        sampler.startSampler(latch, ClientRunBasis.time, 1000);
        samplerLatch.await();
        assertEquals(1000, (long) sampler.getDuration());
        assertEquals(10, (long) sampler.getRampUpTime());
        assertEquals(20, (long) sampler.getRampDownTime());
    }
}