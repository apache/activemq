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

public class CronParserTest {

    @Test
    public void testgetNextTimeMinutes() throws MessageFormatException {
        String test = "30 * * * *";
        long current = 20*60*1000;
        Calendar calender = Calendar.getInstance();
        calender.setTimeInMillis(current);
        System.out.println("start:" + calender.getTime());
        long next = CronParser.getNextScheduledTime(test, current);
        
        calender.setTimeInMillis(next);
        System.out.println("next:" + calender.getTime());
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
        long expected = 60*1000*60*8;
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
            CronParser.validate("30 08 10 06 * ");
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

    }

    public void testGetNextScheduledTime() {
        fail("Not yet implemented");
    }

    CronParser.CronEntry createEntry(String str, int start, int end) {
        return new CronParser.CronEntry("test", str, start, end);
    }

}
