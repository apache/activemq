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
package org.apache.activemq.filter.function;

import org.apache.activemq.filter.FunctionCallExpression;
import org.apache.activemq.filter.MessageEvaluationContext;

/**
 * Interface required for objects that will be registered as functions for use in selectors.  Handles parse-
 * time and evaluation-time operations.
 */
public interface FilterFunction {
    /**
     * Check whether the function, as it is used, is valid.  Checking arguments here will return errors
     * to clients at the time invalid selectors are initially specified, rather than waiting until the selector is
     * applied to a message.
     *
     * @param    FunctionCallExpression expr - the full expression of the function call, as used.
     * @return true - if the function call is valid; false - otherwise.
     */
    public boolean isValid(FunctionCallExpression expr);

    /**
     * Determine whether the function, as it will be called, returns a boolean value.  Called during
     * expression parsing after the full expression for the function call, including its arguments, has
     * been parsed.  This allows functions with variable return types to function as boolean expressions in
     * selectors without sacrificing parse-time checking.
     *
     * @param    FunctionCallExpression expr - the full expression of the function call, as used.
     * @return true - if the function returns a boolean value for its use in the given expression;
     * false - otherwise.
     */
    public boolean returnsBoolean(FunctionCallExpression expr);


    /**
     * Evaluate the function call in the given context.  The arguments must be evaluated, as-needed, by the
     * function.  Note that boolean expressions must return Boolean objects.
     *
     * @param    FunctionCallExpression expr - the full expression of the function call, as used.
     * @param    MessageEvaluationContext message - the context within which to evaluate the call.
     */
    public Object evaluate(FunctionCallExpression expr, MessageEvaluationContext message)
            throws javax.jms.JMSException;
}

