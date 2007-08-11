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
package org.apache.activemq.systest;

import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestResult;

/**
 * A JUnit {@link Test} for running a Scenario in a JUnit test suite.
 * 
 * @version $Revision: 1.1 $
 */
public class ScenarioTestCase implements Test {

    private Scenario scenario;
    private String description;

    public ScenarioTestCase(Scenario scenario, String description) {
        this.scenario = scenario;
        this.description = description;
    }

    public int countTestCases() {
        return 1;
    }

    public void run(TestResult result) {
        result.startTest(this);
        try {
            scenario.start();
            scenario.run();
            scenario.stop();
            result.endTest(this);
        }
        catch (AssertionFailedError e) {
            result.addFailure(this, e);
        }
        catch (Throwable e) {
            System.out.println("Failed to run test: " + e);
            e.printStackTrace();
            result.addError(this, e);
        }
        finally {
            try {
                scenario.stop();
                scenario = null;
            }
            catch (Exception e) {
                System.out.println("Failed to close down test: " + e);
                e.printStackTrace();
            }
        }
    }

    public String toString() {
        return description;
    }
}
