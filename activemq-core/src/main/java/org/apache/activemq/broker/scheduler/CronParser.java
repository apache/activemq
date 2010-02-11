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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import javax.jms.MessageFormatException;

public class CronParser {
    private static final int NUMBER_TOKENS = 5;
    private static final int MINUTES = 0;
    private static final int HOURS = 1;
    private static final int DAY_OF_MONTH = 2;
    private static final int MONTH = 3;
    private static final int DAY_OF_WEEK = 4;

    public static long getNextScheduledTime(final String cronEntry, long currentTime) throws MessageFormatException {
        long result = 0;
        if (cronEntry != null && cronEntry.length() > 0) {
            List<String> list = tokenize(cronEntry);
            List<CronEntry> entries = buildCronEntries(list);
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(currentTime);
            int currentMinutes = calendar.get(Calendar.MINUTE);
            int currentHours = calendar.get(Calendar.HOUR_OF_DAY);
            int currentDayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
            int currentMonth = calendar.get(Calendar.MONTH) + 1;
            int currentDayOfWeek = calendar.get(Calendar.DAY_OF_WEEK) - 1;

            CronEntry minutes = entries.get(MINUTES);
            CronEntry hours = entries.get(HOURS);
            CronEntry dayOfMonth = entries.get(DAY_OF_MONTH);
            CronEntry month = entries.get(MONTH);
            CronEntry dayOfWeek = entries.get(DAY_OF_MONTH);
            if (!isCurrent(month, currentMonth)) {
                int nextMonth = getNext(month, currentMonth);
                Calendar working = (Calendar) calendar.clone();
                working.add(Calendar.MONTH, nextMonth);
                result += working.getTimeInMillis();
            }
            if (!isCurrent(dayOfMonth, currentDayOfMonth)) {
                int nextDay = getNext(dayOfMonth, currentMonth);
                Calendar working = (Calendar) calendar.clone();
                working.add(Calendar.DAY_OF_MONTH, nextDay);
                result += working.getTimeInMillis();
            }
            if (!isCurrent(dayOfWeek, currentDayOfWeek)) {
                int nextDay = getNext(dayOfWeek, currentDayOfWeek);
                Calendar working = (Calendar) calendar.clone();
                working.add(Calendar.DAY_OF_WEEK, nextDay);
                result += working.getTimeInMillis();
            }
            if (!isCurrent(hours, currentHours)) {
                int nextHour = getNext(hours, currentHours);
                Calendar working = (Calendar) calendar.clone();
                working.add(Calendar.HOUR_OF_DAY, nextHour);
                result += working.getTimeInMillis();
            }
            if (!isCurrent(minutes, currentMinutes)) {
                int nextMinutes = getNext(minutes, currentMinutes);
                Calendar working = (Calendar) calendar.clone();
                working.add(Calendar.MINUTE, nextMinutes);
                result += working.getTimeInMillis();
            }
            if (result == 0) {
                // this can occur for "* * * * *"
                result = currentTime + 60 * 1000;
                result = result / 1000 * 1000;
            }
        }
        return result;
    }

    static List<String> tokenize(String cron) throws IllegalArgumentException {
        StringTokenizer tokenize = new StringTokenizer(cron);
        List<String> result = new ArrayList<String>();
        while (tokenize.hasMoreTokens()) {
            result.add(tokenize.nextToken());
        }
        if (result.size() != NUMBER_TOKENS) {
            throw new IllegalArgumentException("Not a valid cron entry - wrong number of tokens(" + result.size()
                    + "): " + cron);
        }
        return result;
    }

    public static void validate(final String cronEntry) throws MessageFormatException {
        List<String> list = tokenize(cronEntry);
        List<CronEntry> entries = buildCronEntries(list);
        for (CronEntry e : entries) {
            validate(e);
        }
    }

    static void validate(CronEntry entry) throws MessageFormatException {

        List<Integer> list = calculateValues(entry);
        if (list.isEmpty() || list.get(0).intValue() < entry.start || list.get(list.size() - 1).intValue() > entry.end) {
            throw new MessageFormatException("Invalid token: " + entry);
        }
    }

    static int getNext(final CronEntry entry, final int current) throws MessageFormatException {
        int result = 0;
        List<Integer> list = calculateValues(entry);
        Collections.sort(list);
        int next = -1;
        for (Integer i : list) {
            if (i.intValue() > current) {
                next = i.intValue();
                break;
            }
        }
        if (next != -1) {
            result = next - current;
        } else {
            int first = list.get(0).intValue();
            result = entry.end + first - entry.start - current;
        }

        return result;
    }

    static boolean isCurrent(final CronEntry entry, final int current) throws MessageFormatException {

        List<Integer> list = calculateValues(entry);
        boolean result = list.contains(new Integer(current));
        return result;
    }

    protected static List<Integer> calculateValues(CronEntry entry) {
        List<Integer> result = new ArrayList<Integer>();
        if (isAll(entry.token)) {
            for (int i = entry.start; i < entry.end; i++) {
                result.add(i);
            }
        } else if (isAStep(entry.token)) {
            int denominator = getDenominator(entry.token);
            String numerator = getNumerator(entry.token);
            CronEntry ce = new CronEntry(entry.name, numerator, entry.start, entry.end);
            List<Integer> list = calculateValues(ce);
            for (Integer i : list) {
                if (i.intValue() % denominator == 0) {
                    result.add(i);
                }
            }
        } else if (isAList(entry.token)) {
            StringTokenizer tokenizer = new StringTokenizer(entry.token, ",");
            while (tokenizer.hasMoreTokens()) {
                String str = tokenizer.nextToken();
                CronEntry ce = new CronEntry(entry.name, str, entry.start, entry.end);
                List<Integer> list = calculateValues(ce);
                result.addAll(list);
            }
        } else if (isARange(entry.token)) {
            int index = entry.token.indexOf('-');
            int first = Integer.parseInt(entry.token.substring(0, index));
            int last = Integer.parseInt(entry.token.substring(index + 1));
            for (int i = first; i <= last; i++) {
                result.add(i);
            }
        } else {
            int value = Integer.parseInt(entry.token);
            result.add(value);
        }
        return result;
    }

    protected static boolean isARange(String token) {
        return token != null && token.indexOf('-') >= 0;
    }

    protected static boolean isAStep(String token) {
        return token != null && token.indexOf('/') >= 0;
    }

    protected static boolean isAList(String token) {
        return token != null && token.indexOf(',') >= 0;
    }

    protected static boolean isAll(String token) {
        return token != null && token.length() == 1 && token.charAt(0) == '*';
    }

    protected static int getDenominator(final String token) {
        int result = 0;
        int index = token.indexOf('/');
        String str = token.substring(index + 1);
        result = Integer.parseInt(str);
        return result;
    }

    protected static String getNumerator(final String token) {
        int index = token.indexOf('/');
        String str = token.substring(0, index);
        return str;
    }

    static List<CronEntry> buildCronEntries(List<String> tokens) {
        List<CronEntry> result = new ArrayList<CronEntry>();
        CronEntry minutes = new CronEntry("Minutes", tokens.get(MINUTES), 0, 59);
        result.add(minutes);
        CronEntry hours = new CronEntry("Hours", tokens.get(HOURS), 0, 23);
        result.add(hours);
        CronEntry dayOfMonth = new CronEntry("DayOfMonth", tokens.get(DAY_OF_MONTH), 1, 31);
        result.add(dayOfMonth);
        CronEntry month = new CronEntry("Month", tokens.get(MONTH), 1, 12);
        result.add(month);
        CronEntry dayOfWeek = new CronEntry("DayOfWeek", tokens.get(DAY_OF_WEEK), 0, 6);
        result.add(dayOfWeek);
        return result;
    }

    static class CronEntry {
        final String name;
        final String token;
        final int start;
        final int end;
        CronEntry(String name, String token, int start, int end) {
            this.name = name;
            this.token = token;
            this.start = start;
            this.end = end;
        }
        @Override
        public String toString() {
            return this.name + ":" + token;
        }
    }

}
