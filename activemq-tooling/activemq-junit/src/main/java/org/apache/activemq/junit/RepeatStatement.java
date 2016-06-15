/*
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
package org.apache.activemq.junit;

import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RepeatStatement extends Statement {

    private static final Logger LOG = LoggerFactory.getLogger(RepeatStatement.class);

    private final int repetitions;
    private final boolean untilFailure;
    private final Statement statement;

    public static Builder builder() {
        return new Builder();
    }

    public RepeatStatement(int times, boolean untilFailure, Statement statement) {
        this.repetitions = times;
        this.untilFailure = untilFailure;
        this.statement = statement;
    }

    protected RepeatStatement(Builder builder, Statement next) {
        this.repetitions = builder.getRepetitions();
        this.untilFailure = builder.isUntilFailure();
        this.statement = next;
    }

    @Override
    public void evaluate() throws Throwable {
        for (int i = 0; i < repetitions && !untilFailure; i++) {
            if (untilFailure) {
                LOG.info("Running test iteration: {}.", i + 1);
            } else {
                LOG.info("Running test iteration: {} of configured repetitions: {}", i + 1, repetitions);
            }
            statement.evaluate();
        }
    }

    /**
     * Builder for {@link Repeat}.
     */
    public static class Builder {
        private int repetitions = 1;
        private boolean untilFailure = false;

        protected Builder() {}

        /**
         * Specifies the number of times to run the test.
         *
         * @param repetitions
         *      The number of times to run the test.
         *
         * @return {@code this} for method chaining.
         */
        public Builder withRepetitions(int repetitions) {
            if (repetitions <= 0) {
                throw new IllegalArgumentException("repetitions must be greater than zero");
            }

            this.repetitions = repetitions;
            return this;
        }

        /**
         * Specifies the number of times to run the test.
         *
         * @param untilFailure
         *      true if the test should run until a failure occurs.
         *
         * @return {@code this} for method chaining.
         */
        public Builder withRunUntilFailure(boolean untilFailure) {
            this.untilFailure = untilFailure;
            return this;
        }

        protected int getRepetitions() {
            return repetitions;
        }

        protected boolean isUntilFailure()  {
            return untilFailure;
        }

        /**
         * Builds a {@link RepeatStatement} instance using the values in this builder.
         *
         * @param next
         *      The statement instance to wrap with the newly create repeat statement.
         *
         * @return a new {@link RepeatStatement} that wraps the given {@link Statement}.
         */
        public RepeatStatement build(Statement next) {
            if (next == null) {
                throw new NullPointerException("statement cannot be null");
            }

            return new RepeatStatement(this, next);
        }

        /**
         * Builds a {@link RepeatStatement} instance using the values in this builder.
         *
         * @param annotation
         *      The {@link Repeat} annotation that triggered this statement being created.
         * @param next
         *      The statement instance to wrap with the newly create repeat statement.
         *
         * @return a new {@link RepeatStatement} that wraps the given {@link Statement}.
         */
        public RepeatStatement build(Repeat annotation, Statement next) {
            if (next == null) {
                throw new NullPointerException("statement cannot be null");
            }

            if (annotation == null) {
                throw new NullPointerException("annotation cannot be null");
            }

            withRepetitions(annotation.repetitions());
            withRunUntilFailure(annotation.untilFailure());

            return new RepeatStatement(this, next);
        }
    }
}