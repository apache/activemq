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

        if (cronEntry == null || cronEntry.length() == 0) {
            return result;
        }

        // Handle the once per minute case "* * * * *"
        // starting the next event at the top of the minute.
        if (cronEntry.startsWith("* * * * *")) {
            result = currentTime + 60 * 1000;
            result = result / 1000 * 1000;
            return result;
        }

        List<String> list = tokenize(cronEntry);
        List<CronEntry> entries = buildCronEntries(list);
        Calendar working = Calendar.getInstance();
        working.setTimeInMillis(currentTime);
        working.set(Calendar.SECOND, 0);

        CronEntry minutes = entries.get(MINUTES);
        CronEntry hours = entries.get(HOURS);
        CronEntry dayOfMonth = entries.get(DAY_OF_MONTH);
        CronEntry month = entries.get(MONTH);
        CronEntry dayOfWeek = entries.get(DAY_OF_WEEK);

        // Start at the top of the next minute, cron is only guaranteed to be
        // run on the minute.
        int timeToNextMinute = 60 - working.get(Calendar.SECOND);
        working.add(Calendar.SECOND, timeToNextMinute);

        // If its already to late in the day this will roll us over to tomorrow
        // so we'll need to check again when done updating month and day.
        int currentMinutes = working.get(Calendar.MINUTE);
        if (!isCurrent(minutes, currentMinutes)) {
            int nextMinutes = getNext(minutes, currentMinutes);
            working.add(Calendar.MINUTE, nextMinutes);
        }

        int currentHours = working.get(Calendar.HOUR_OF_DAY);
        if (!isCurrent(hours, currentHours)) {
            int nextHour = getNext(hours, currentHours);
            working.add(Calendar.HOUR_OF_DAY, nextHour);
        }

        // We can roll into the next month here which might violate the cron setting
        // rules so we check once then recheck again after applying the month settings.
        doUpdateCurrentDay(working, dayOfMonth, dayOfWeek);

        // Start by checking if we are in the right month, if not then calculations
        // need to start from the beginning of the month to ensure that we don't end
        // up on the wrong day.  (Can happen when DAY_OF_WEEK is set and current time
        // is ahead of the day of the week to execute on).
        doUpdateCurrentMonth(working, month);

        // Now Check day of week and day of month together since they can be specified
        // together in one entry, if both "day of month" and "day of week" are restricted
        // (not "*"), then either the "day of month" field (3) or the "day of week" field
        // (5) must match the current day or the Calenday must be advanced.
        doUpdateCurrentDay(working, dayOfMonth, dayOfWeek);

        // Now we can chose the correct hour and minute of the day in question.

        currentHours = working.get(Calendar.HOUR_OF_DAY);
        if (!isCurrent(hours, currentHours)) {
            int nextHour = getNext(hours, currentHours);
            working.add(Calendar.HOUR_OF_DAY, nextHour);
        }

        currentMinutes = working.get(Calendar.MINUTE);
        if (!isCurrent(minutes, currentMinutes)) {
            int nextMinutes = getNext(minutes, currentMinutes);
            working.add(Calendar.MINUTE, nextMinutes);
        }

        result = working.getTimeInMillis();

        if (result <= currentTime) {
            throw new ArithmeticException("Unable to compute next scheduled exection time.");
        }

        return result;
    }

    protected static long doUpdateCurrentMonth(Calendar working, CronEntry month) throws MessageFormatException {

        int currentMonth = working.get(Calendar.MONTH) + 1;
        if (!isCurrent(month, currentMonth)) {
            int nextMonth = getNext(month, currentMonth);
            working.add(Calendar.MONTH, nextMonth);

            // Reset to start of month.
            resetToStartOfDay(working, 1);

            return working.getTimeInMillis();
        }

        return 0L;
    }

    protected static long doUpdateCurrentDay(Calendar working, CronEntry dayOfMonth, CronEntry dayOfWeek) throws MessageFormatException {

        int currentDayOfWeek = working.get(Calendar.DAY_OF_WEEK) - 1;
        int currentDayOfMonth = working.get(Calendar.DAY_OF_MONTH);

        // Simplest case, both are unrestricted or both match today otherwise
        // result must be the closer of the two if both are set, or the next
        // match to the one that is.
        if (!isCurrent(dayOfWeek, currentDayOfWeek) ||
            !isCurrent(dayOfMonth, currentDayOfMonth) ) {

            int nextWeekDay = Integer.MAX_VALUE;
            int nextCalendarDay = Integer.MAX_VALUE;

            if (!isCurrent(dayOfWeek, currentDayOfWeek)) {
                nextWeekDay = getNext(dayOfWeek, currentDayOfWeek);
            }

            if (!isCurrent(dayOfMonth, currentDayOfMonth)) {
                nextCalendarDay = getNext(dayOfMonth, currentDayOfMonth);
            }

            if( nextWeekDay < nextCalendarDay ) {
                working.add(Calendar.DAY_OF_WEEK, nextWeekDay);
            } else {
                working.add(Calendar.DAY_OF_MONTH, nextCalendarDay);
            }

            // Since the day changed, we restart the clock at the start of the day
            // so that the next time will either be at 12am + value of hours and
            // minutes pattern.
            resetToStartOfDay(working, working.get(Calendar.DAY_OF_MONTH));

            return working.getTimeInMillis();
        }

        return 0L;
    }

    public static void validate(final String cronEntry) throws MessageFormatException {
        List<String> list = tokenize(cronEntry);
        List<CronEntry> entries = buildCronEntries(list);
        for (CronEntry e : entries) {
            validate(e);
        }
    }

    static void validate(final CronEntry entry) throws MessageFormatException {

        List<Integer> list = entry.currentWhen;
        if (list.isEmpty() || list.get(0).intValue() < entry.start || list.get(list.size() - 1).intValue() > entry.end) {
            throw new MessageFormatException("Invalid token: " + entry);
        }
    }

    static int getNext(final CronEntry entry, final int current) throws MessageFormatException {
        int result = 0;

        if (entry.currentWhen == null) {
            entry.currentWhen = calculateValues(entry);
        }

        List<Integer> list = entry.currentWhen;
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

            // Account for difference of one vs zero based indices.
            if (entry.name.equals("DayOfWeek") || entry.name.equals("Month")) {
                result++;
            }
        }

        return result;
    }

    static boolean isCurrent(final CronEntry entry, final int current) throws MessageFormatException {
        boolean result = entry.currentWhen.contains(new Integer(current));
        return result;
    }

    protected static void resetToStartOfDay(Calendar target, int day) {
        target.set(Calendar.DAY_OF_MONTH, day);
        target.set(Calendar.HOUR_OF_DAY, 0);
        target.set(Calendar.MINUTE, 0);
        target.set(Calendar.SECOND, 0);
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

    protected static List<Integer> calculateValues(final CronEntry entry) {
        List<Integer> result = new ArrayList<Integer>();
        if (isAll(entry.token)) {
            for (int i = entry.start; i <= entry.end; i++) {
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
        Collections.sort(result);
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

        CronEntry minutes = new CronEntry("Minutes", tokens.get(MINUTES), 0, 60);
        minutes.currentWhen = calculateValues(minutes);
        result.add(minutes);
        CronEntry hours = new CronEntry("Hours", tokens.get(HOURS), 0, 24);
        hours.currentWhen = calculateValues(hours);
        result.add(hours);
        CronEntry dayOfMonth = new CronEntry("DayOfMonth", tokens.get(DAY_OF_MONTH), 1, 31);
        dayOfMonth.currentWhen = calculateValues(dayOfMonth);
        result.add(dayOfMonth);
        CronEntry month = new CronEntry("Month", tokens.get(MONTH), 1, 12);
        month.currentWhen = calculateValues(month);
        result.add(month);
        CronEntry dayOfWeek = new CronEntry("DayOfWeek", tokens.get(DAY_OF_WEEK), 0, 6);
        dayOfWeek.currentWhen = calculateValues(dayOfWeek);
        result.add(dayOfWeek);

        return result;
    }

    static class CronEntry {

        final String name;
        final String token;
        final int start;
        final int end;

        List<Integer> currentWhen;

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
