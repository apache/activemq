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
package org.apache.activemq.usage;

/**
 * Internal API. The absolute usage-value bounds {@code [lower, upper)} of the range that maps
 * to a Usage's current percentUsage bucket. Lock-free hot paths compare a usage value against
 * these two longs - no division or percent math - and only enter the locked
 * percent-recompute path when the value crosses out of the bucket.
 *
 * <p>Immutable, and held by {@link Usage} in a single volatile reference so the pair can never
 * tear (two independent volatile longs could be read as a wider interval and miss a real
 * crossing).
 *
 * <p>The bucket math matches {@code Usage.caclPercentUsage()} truncating-division semantics:
 * percent P (a multiple of percentUsageMinDelta d, against limit L) covers values in
 * {@code [ceil(P*L/100), ceil((P+d)*L/100))}. Bounds only need to be conservative - the locked
 * path always derives the percent from {@code caclPercentUsage()}, so an imprecise bound costs
 * at most an extra locked recompute, never a wrong percent.
 */
public final class PercentBounds {

    /**
     * Sentinel whose range is empty, so any value registers as a crossing - forces the first
     * observation to take the locked initialization path.
     */
    public static final PercentBounds ALWAYS_CROSS = new PercentBounds(0, 0);

    public final long lower;
    public final long upper;

    PercentBounds(long lower, long upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public boolean contains(long value) {
        return value >= lower && value < upper;
    }

    /**
     * Bounds of the usage-value range that maps to the given percent bucket.
     * {@code limit == 0} pins the percent at 0 (matching caclPercentUsage), so the bucket is
     * unbounded. Negative percents (negative usage is an accounting-error state) collapse into
     * one bucket below zero so any recovery to {@code >= 0} re-enters the locked path.
     */
    public static PercentBounds compute(int percent, long limit, int minDelta) {
        if (limit == 0) {
            return new PercentBounds(Long.MIN_VALUE, Long.MAX_VALUE);
        }
        if (percent < 0) {
            return new PercentBounds(Long.MIN_VALUE, 0);
        }
        final int delta = Math.max(1, minDelta);
        final long lower = percent == 0 ? 0 : ceilDivSaturated(percent, limit);
        final long upper = ceilDivSaturated((long) percent + delta, limit);
        return new PercentBounds(lower, upper);
    }

    /** ceil(percent * limit / 100), saturating to Long.MAX_VALUE on overflow. */
    private static long ceilDivSaturated(long percent, long limit) {
        try {
            final long product = Math.multiplyExact(percent, limit);
            return product / 100 + (product % 100 == 0 ? 0 : 1);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public String toString() {
        return "PercentBounds[" + lower + "," + upper + ")";
    }
}
