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

package org.apache.activemq.broker.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.openmbean.TabularData;

import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.jmx.JobSchedulerViewMBean;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests of the JMX JobSchedulerStore management MBean.
 */
public class JobSchedulerJmxManagementTests extends JobSchedulerTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JobSchedulerJmxManagementTests.class);

    @Test
    public void testJobSchedulerMBeanIsRegistered() throws Exception {
        JobSchedulerViewMBean view = getJobSchedulerMBean();
        assertNotNull(view);
        assertTrue(view.getAllJobs().isEmpty());
    }

    @Test
    public void testGetNumberOfJobs() throws Exception {
        JobSchedulerViewMBean view = getJobSchedulerMBean();
        assertNotNull(view);
        assertTrue(view.getAllJobs().isEmpty());
        scheduleMessage(60000, -1, -1);
        assertFalse(view.getAllJobs().isEmpty());
        assertEquals(1, view.getAllJobs().size());
        scheduleMessage(60000, -1, -1);
        assertEquals(2, view.getAllJobs().size());
    }

    @Test
    public void testRemvoeJob() throws Exception {
        JobSchedulerViewMBean view = getJobSchedulerMBean();
        assertNotNull(view);
        assertTrue(view.getAllJobs().isEmpty());
        scheduleMessage(60000, -1, -1);
        assertFalse(view.getAllJobs().isEmpty());
        TabularData jobs = view.getAllJobs();
        assertEquals(1, jobs.size());
        for (Object key : jobs.keySet()) {
            String jobId = ((List<?>)key).get(0).toString();
            LOG.info("Attempting to remove Job: {}", jobId);
            view.removeJob(jobId);
        }
        assertTrue(view.getAllJobs().isEmpty());
    }

    @Test
    public void testRemvoeJobInRange() throws Exception {
        JobSchedulerViewMBean view = getJobSchedulerMBean();
        assertNotNull(view);
        assertTrue(view.getAllJobs().isEmpty());
        scheduleMessage(60000, -1, -1);
        assertFalse(view.getAllJobs().isEmpty());
        String now = JobSupport.getDateTime(System.currentTimeMillis());
        String later = JobSupport.getDateTime(System.currentTimeMillis() + 120 * 1000);
        view.removeAllJobs(now, later);
        assertTrue(view.getAllJobs().isEmpty());
    }

    @Test
    public void testGetNextScheduledJob() throws Exception {
        JobSchedulerViewMBean view = getJobSchedulerMBean();
        assertNotNull(view);
        assertTrue(view.getAllJobs().isEmpty());
        scheduleMessage(60000, -1, -1);
        assertFalse(view.getAllJobs().isEmpty());
        long before = System.currentTimeMillis() + 57 * 1000;
        long toLate = System.currentTimeMillis() + 63 * 1000;
        String next = view.getNextScheduleTime();
        long nextTime = JobSupport.getDataTime(next);
        LOG.info("Next Scheduled Time: {} should be after: {}", next, JobSupport.getDateTime(before));
        assertTrue(nextTime > before);
        assertTrue(nextTime < toLate);
    }

    @Test
    public void testGetExecutionCount() throws Exception {
        final JobSchedulerViewMBean view = getJobSchedulerMBean();
        assertNotNull(view);
        assertTrue(view.getAllJobs().isEmpty());
        scheduleMessage(10000, 1000, 10);
        assertFalse(view.getAllJobs().isEmpty());
        TabularData jobs = view.getAllJobs();
        assertEquals(1, jobs.size());
        String jobId = null;
        for (Object key : jobs.keySet()) {
            jobId = ((List<?>)key).get(0).toString();
        }

        final String fixedJobId = jobId;
        LOG.info("Attempting to get execution count for Job: {}", jobId);
        assertEquals(0, view.getExecutionCount(jobId));

        assertTrue("Should execute again", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return view.getExecutionCount(fixedJobId) > 0;
            }
        }));
    }

    @Override
    protected boolean isUseJmx() {
        return true;
    }

    protected void scheduleMessage(int time, int period, int repeat) throws Exception {
        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage("test msg");
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, period);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, repeat);
        producer.send(message);
        connection.close();
    }
}
