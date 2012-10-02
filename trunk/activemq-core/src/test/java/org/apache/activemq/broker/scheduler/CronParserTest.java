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

import static org.junit.Assert.fail;

import java.util.Calendar;
import java.util.List;
import javax.jms.MessageFormatException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CronParserTest {

    private static final Logger LOG = LoggerFactory.getLogger(CronParserTest.class);

    @Test
    public void testgetNextTimeDayOfWeek() throws MessageFormatException {

        // using an absolute date so that result will be absolute - Monday 15 Nov 2010
        Calendar current = Calendar.getInstance();
        current.set(2010, Calendar.NOVEMBER, 15, 9, 15, 30);
        LOG.debug("start:" + current.getTime());

        String test = "* * * * 5";
        long next = CronParser.getNextScheduledTime(test, current.getTimeInMillis());

        Calendar result = Calendar.getInstance();
        result.setTimeInMillis(next);
        LOG.debug("next:" + result.getTime());

        assertEquals(0,result.get(Calendar.SECOND));
        assertEquals(0,result.get(Calendar.MINUTE));
        assertEquals(0,result.get(Calendar.HOUR));
        // expecting Friday 19th
        assertEquals(19,result.get(Calendar.DAY_OF_MONTH));
        assertEquals(Calendar.NOVEMBER,result.get(Calendar.MONTH));
        assertEquals(2010,result.get(Calendar.YEAR));
    }

    @Test
    public void testgetNextTimeDayOfWeekVariant() throws MessageFormatException {

        // using an absolute date so that result will be absolute - Monday 7 March 2011
        Calendar current = Calendar.getInstance();
        current.set(2011, Calendar.MARCH, 7, 9, 15, 30);
        LOG.debug("start:" + current.getTime());

        String test = "50 20 * * 5";
        long next = CronParser.getNextScheduledTime(test, current.getTimeInMillis());

        Calendar result = Calendar.getInstance();
        result.setTimeInMillis(next);
        LOG.debug("next:" + result.getTime());

        assertEquals(0,result.get(Calendar.SECOND));
        assertEquals(50,result.get(Calendar.MINUTE));
        assertEquals(20,result.get(Calendar.HOUR_OF_DAY));
        // expecting Friday 11th
        assertEquals(11,result.get(Calendar.DAY_OF_MONTH));
        assertEquals(Calendar.FRIDAY,result.get(Calendar.DAY_OF_WEEK));
        assertEquals(Calendar.MARCH,result.get(Calendar.MONTH));
        assertEquals(2011,result.get(Calendar.YEAR));

        // Match to the day of week, but to late to run, should just a week forward.
        current = Calendar.getInstance();
        current.set(2011, Calendar.MARCH, 11, 22, 0, 30);
        LOG.debug("update:" + current.getTime());

        next = CronParser.getNextScheduledTime(test, current.getTimeInMillis());

        result = Calendar.getInstance();
        result.setTimeInMillis(next);
        LOG.debug("next:" + result.getTime());

        //assertEquals(0,result.get(Calendar.SECOND));
        assertEquals(50,result.get(Calendar.MINUTE));
        assertEquals(20,result.get(Calendar.HOUR_OF_DAY));
        // expecting Friday 18th
        assertEquals(18,result.get(Calendar.DAY_OF_MONTH));
        assertEquals(Calendar.FRIDAY,result.get(Calendar.DAY_OF_WEEK));
        assertEquals(Calendar.MARCH,result.get(Calendar.MONTH));
        assertEquals(2011,result.get(Calendar.YEAR));
    }

    @Test
    public void testgetNextTimeMonthVariant() throws MessageFormatException {

        // using an absolute date so that result will be absolute - Monday 7 March 2011
        Calendar current = Calendar.getInstance();
        current.set(2011, Calendar.MARCH, 7, 9, 15, 30);
        LOG.debug("start:" + current.getTime());

        String test = "0 20 * 4,5 0";
        long next = CronParser.getNextScheduledTime(test, current.getTimeInMillis());

        Calendar result = Calendar.getInstance();
        result.setTimeInMillis(next);
        LOG.debug("next:" + result.getTime());

        assertEquals(0,result.get(Calendar.SECOND));
        assertEquals(0,result.get(Calendar.MINUTE));
        assertEquals(20,result.get(Calendar.HOUR_OF_DAY));
        // expecting Sunday 3rd of April
        assertEquals(Calendar.APRIL,result.get(Calendar.MONTH));
        assertEquals(3,result.get(Calendar.DAY_OF_MONTH));
        assertEquals(Calendar.SUNDAY,result.get(Calendar.DAY_OF_WEEK));
        assertEquals(2011,result.get(Calendar.YEAR));

        current = Calendar.getInstance();
        current.set(2011, Calendar.APRIL, 30, 22, 0, 30);
        LOG.debug("update:" + current.getTime());

        next = CronParser.getNextScheduledTime(test, current.getTimeInMillis());

        result = Calendar.getInstance();
        result.setTimeInMillis(next);
        LOG.debug("next:" + result.getTime());

        assertEquals(0,result.get(Calendar.SECOND));
        assertEquals(0,result.get(Calendar.MINUTE));
        assertEquals(20,result.get(Calendar.HOUR_OF_DAY));
        // expecting Sunday 1st of May
        assertEquals(1,result.get(Calendar.DAY_OF_MONTH));
        assertEquals(Calendar.SUNDAY,result.get(Calendar.DAY_OF_WEEK));
        assertEquals(Calendar.MAY,result.get(Calendar.MONTH));
        assertEquals(2011,result.get(Calendar.YEAR));

        // Move past last time and see if reschedule to next year works.
        current = Calendar.getInstance();
        current.set(2011, Calendar.MAY, 30, 22, 0, 30);
        LOG.debug("update:" + current.getTime());

        next = CronParser.getNextScheduledTime(test, current.getTimeInMillis());

        result = Calendar.getInstance();
        result.setTimeInMillis(next);
        LOG.debug("next:" + result.getTime());

        assertEquals(0,result.get(Calendar.SECOND));
        assertEquals(0,result.get(Calendar.MINUTE));
        assertEquals(20,result.get(Calendar.HOUR_OF_DAY));
        // expecting Sunday 1st of April - 2012
        assertEquals(1,result.get(Calendar.DAY_OF_MONTH));
        assertEquals(Calendar.SUNDAY,result.get(Calendar.DAY_OF_WEEK));
        assertEquals(Calendar.APRIL,result.get(Calendar.MONTH));
        assertEquals(2012,result.get(Calendar.YEAR));
    }

    @Test
    public void testgetNextTimeMonth() throws MessageFormatException {

        // using an absolute date so that result will be absolute - Monday 15 Nov 2010
        Calendar current = Calendar.getInstance();
        current.set(2010, Calendar.NOVEMBER, 15, 9, 15, 30);
        LOG.debug("start:" + current.getTime());

        String test = "* * * 12 *";
        long next = CronParser.getNextScheduledTime(test, current.getTimeInMillis());

        Calendar result = Calendar.getInstance();
        result.setTimeInMillis(next);
        LOG.debug("next:" + result.getTime());

        assertEquals(0,result.get(Calendar.SECOND));
        assertEquals(0,result.get(Calendar.MINUTE));
        assertEquals(0,result.get(Calendar.HOUR_OF_DAY));
        assertEquals(1,result.get(Calendar.DAY_OF_MONTH));
        assertEquals(Calendar.DECEMBER,result.get(Calendar.MONTH));
        assertEquals(2010,result.get(Calendar.YEAR));
    }

    @Test
    public void testgetNextTimeDays() throws MessageFormatException {

        // using an absolute date so that result will be absolute - Monday 15 Nov 2010
        Calendar current = Calendar.getInstance();
        current.set(2010, Calendar.NOVEMBER, 15, 9, 15, 30);
        LOG.debug("start:" + current.getTime());

        String test = "* * 16 * *";
        long next = CronParser.getNextScheduledTime(test, current.getTimeInMillis());

        Calendar result = Calendar.getInstance();
        result.setTimeInMillis(next);
        LOG.debug("next:" + result.getTime());

        assertEquals(0,result.get(Calendar.SECOND));
        assertEquals(0,result.get(Calendar.MINUTE));
        assertEquals(0,result.get(Calendar.HOUR));
        assertEquals(16,result.get(Calendar.DAY_OF_MONTH));
        assertEquals(Calendar.NOVEMBER,result.get(Calendar.MONTH));
        assertEquals(2010,result.get(Calendar.YEAR));
    }

    @Test
    public void testgetNextTimeMinutes() throws MessageFormatException {
        String test = "30 * * * *";
        long current = 20*60*1000;
        Calendar calender = Calendar.getInstance();
        calender.setTimeInMillis(current);
        LOG.debug("start:" + calender.getTime());
        long next = CronParser.getNextScheduledTime(test, current);

        calender.setTimeInMillis(next);
        LOG.debug("next:" + calender.getTime());
        long result = next - current;
        assertEquals(60*10*1000,result);
    }

    @Test
    public void testgetNextTimeHours() throws MessageFormatException {
        String test = "* 1 * * *";

        Calendar calender = Calendar.getInstance();
        calender.set(1972, 2, 2, 17, 10, 0);
        long current = calender.getTimeInMillis();
        long next = CronParser.getNextScheduledTime(test, current);

        calender.setTimeInMillis(next);
        long result = next - current;
        long expected = 60*1000*60*8 + 60 * 1000;
        assertEquals(expected,result);
    }

    @Test
    public void testgetNextTimeHoursZeroMin() throws MessageFormatException {
        String test = "0 1 * * *";

        Calendar calender = Calendar.getInstance();
        calender.set(1972, 2, 2, 17, 10, 0);
        long current = calender.getTimeInMillis();
        long next = CronParser.getNextScheduledTime(test, current);

        calender.setTimeInMillis(next);
        long result = next - current;
        long expected = 60*1000*60*7 + 60*1000*50;
        assertEquals(expected,result);
    }

    @Test
    public void testValidate() {
        try {
            CronParser.validate("30 08 10 06 ? ");
            CronParser.validate("30 08 ? 06 5 ");
            CronParser.validate("30 08 ? 06 * ");
            CronParser.validate("* * * * * ");
            CronParser.validate("* * * * 1-6 ");
            CronParser.validate("* * * * 1,2,5 ");
            CronParser.validate("*/10 0-4,8-12 * * 1-2,3-6/2 ");

        } catch (Exception e) {
            fail("Should be valid ");
        }

        try {
            CronParser.validate("61 08 10 06 * ");
            fail("Should not be valid ");
        } catch (Exception e) {
        }
        try {
            CronParser.validate("61 08 06 * ");
            fail("Should not be valid ");
        } catch (Exception e) {
        }
    }

    @Test
    public void testGetNextCommaSeparated() throws MessageFormatException {
        String token = "3,5,7";
        // test minimum values
        int next = CronParser.getNext(createEntry(token, 1, 10), 3);
        assertEquals(2, next);
        next = CronParser.getNext(createEntry(token, 1, 10), 8);
        assertEquals(4, next);
        next = CronParser.getNext(createEntry(token, 1, 10), 1);
        assertEquals(2, next);
    }

    @Test
    public void testGetNextRange() throws MessageFormatException {
        String token = "3-5";
        // test minimum values
        int next = CronParser.getNext(createEntry(token, 1, 10), 3);
        assertEquals(1, next);
        next = CronParser.getNext(createEntry(token, 1, 10), 5);
        assertEquals(7, next);
        next = CronParser.getNext(createEntry(token, 1, 10), 6);
        assertEquals(6, next);
        next = CronParser.getNext(createEntry(token, 1, 10), 1);
        assertEquals(2, next);
    }

    @Test
    public void testGetNextExact() throws MessageFormatException {
        String token = "3";
        int next = CronParser.getNext(createEntry(token, 0, 10), 2);
        assertEquals(1, next);
        next = CronParser.getNext(createEntry(token, 0, 10), 3);
        assertEquals(10, next);
        next = CronParser.getNext(createEntry(token, 0, 10), 1);
        assertEquals(2, next);
    }

    @Test
    public void testTokenize() {
        String test = "*/5 * * * *";
        List<String> list = CronParser.tokenize(test);
        assertEquals(list.size(), 5);

        test = "*/5 * * * * *";
        try {
            list = CronParser.tokenize(test);
            fail("Should have throw an exception");
        } catch (Throwable e) {
        }

        test = "*/5 * * * *";
        try {
            list = CronParser.tokenize(test);
            fail("Should have throw an exception");
        } catch (Throwable e) {
        }

        test = "0 1 2 3 4";
        list = CronParser.tokenize(test);
        assertEquals(list.size(), 5);

        assertEquals(list.get(0), "0");
        assertEquals(list.get(1), "1");
        assertEquals(list.get(2), "2");
        assertEquals(list.get(3), "3");
        assertEquals(list.get(4), "4");
    }

    public void testGetNextScheduledTime() {
        fail("Not yet implemented");
    }

    CronParser.CronEntry createEntry(String str, int start, int end) {
        return new CronParser.CronEntry("test", str, start, end);
    }

}
