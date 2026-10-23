/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

const MONTHS = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
];

function ordinal(day: number): string {
  if (day % 100 >= 11 && day % 100 <= 13) {
    return `${day}th`;
  }
  switch (day % 10) {
    case 1: return `${day}st`;
    case 2: return `${day}nd`;
    case 3: return `${day}rd`;
    default: return `${day}th`;
  }
}

/**
 * Formats a date the way the Jekyll `date_to_string: "ordinal", "US"` filter
 * did, e.g. "May 31st, 2026", or "October 22nd, 2025" with long month names.
 */
export function formatDate(date: Date | string, month: 'short' | 'long' = 'short'): string {
  // Blog metadata dates reach the client serialized as ISO strings.
  const iso = typeof date === 'string' ? date : date.toISOString();
  const [year, monthNumber, day] = iso.slice(0, 10).split('-').map(Number);
  const name = MONTHS[monthNumber - 1];
  return `${month === 'short' ? name.slice(0, 3) : name} ${ordinal(day)}, ${year}`;
}
