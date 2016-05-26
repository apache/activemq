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

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.internal.runners.statements.FailOnTimeout;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Custom JUnit test runner for customizing JUnit tests run in ActiveMQ.
 */
public class ActiveMQTestRunner extends BlockJUnit4ClassRunner {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQTestRunner.class);

    public ActiveMQTestRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected Statement methodBlock(final FrameworkMethod method) {
        Statement statement = super.methodBlock(method);

        // Check for repeats needed
        statement = withPotentialRepeat(method, statement);

        return statement;
    }

    /**
     * Perform the same logic as
     * {@link BlockJUnit4ClassRunner#withPotentialTimeout(FrameworkMethod, Object, Statement)}
     * but with additional support for changing the coded timeout with an extended value.
     *
     * @return either a {@link FailOnTimeout}, or the supplied {@link Statement} as appropriate.
     */
    @SuppressWarnings("deprecation")
    @Override
    protected Statement withPotentialTimeout(FrameworkMethod frameworkMethod, Object testInstance, Statement next) {
        long testTimeout = getOriginalTimeout(frameworkMethod);

        if (testTimeout > 0) {
            String multiplierString = System.getProperty("org.apache.activemq.junit.testTimeoutMultiplier");
            double multiplier = 0.0;

            try {
                multiplier = Double.parseDouble(multiplierString);
            } catch (NullPointerException npe) {
            } catch (NumberFormatException nfe) {
                LOG.warn("Ignoring testTimeoutMultiplier not set to a valid value: " + multiplierString);
            }

            if (multiplier > 0.0) {
                LOG.info("Test timeout multiple {} applied to test timeout {}ms: new timeout = {}",
                    multiplier, testTimeout, (long) (testTimeout * multiplier));
                testTimeout = (long) (testTimeout * multiplier);
            }

            next = FailOnTimeout.builder().
                withTimeout(testTimeout, TimeUnit.MILLISECONDS).build(next);
        } else {
            next = super.withPotentialTimeout(frameworkMethod, testInstance, next);
        }

        return next;
    }

    /**
     * Check for the presence of a {@link Repeat} annotation and return a {@link RepeatStatement}
     * to handle executing the test repeated or the original value if not repeating.
     *
     * @return either a {@link RepeatStatement}, or the supplied {@link Statement} as appropriate.
     */
    protected Statement withPotentialRepeat(FrameworkMethod frameworkMethod, Statement next) {

        Repeat repeatAnnotation = frameworkMethod.getAnnotation(Repeat.class);

        if (repeatAnnotation != null) {
            next = RepeatStatement.builder().build(repeatAnnotation, next);
        }

        return next;
    }

    /**
     * Retrieve the original JUnit {@code timeout} from the {@link Test @Test}
     * annotation on the incoming {@linkplain FrameworkMethod test method}.
     *
     * @return the timeout, or {@code 0} if none was specified
     */
    protected long getOriginalTimeout(FrameworkMethod frameworkMethod) {
        Test test = frameworkMethod.getAnnotation(Test.class);
        return (test != null && test.timeout() > 0 ? test.timeout() : 0);
    }
}
