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
 * Function which splits a string into a list of strings given a regular expression for the separator.
 */

public class splitFunction implements FilterFunction {
    /**
     * Check whether the given expression is valid for this function.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return true - if two or three arguments are passed to the function; false - otherwise.
     */

    public boolean isValid(FunctionCallExpression expr) {
        if ((expr.getNumArguments() >= 2) && (expr.getNumArguments() <= 3))
            return true;

        return false;
    }


    /**
     * Indicate that this function does not return a boolean value.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return false - indicating this filter function never evaluates to a boolean result.
     */

    public boolean returnsBoolean(FunctionCallExpression expr) {
        return false;
    }


    /**
     * Evaluate the given expression for this function in the given context.  A list of zero or more strings
     * results from the evaluation.  The result of the evaluation of the first argument is split with the regular
     * expression which results from the evaluation of the second argument.  If a third argument is given, it
     * is an integer which limits the split.  String#split() performs the split.
     * <p/>
     * The first two arguments must be Strings.  If a third is given, it must be an Integer.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return List - a list of Strings resulting from the split.
     */

    public Object evaluate(FunctionCallExpression expr, MessageEvaluationContext message_ctx)
            throws javax.jms.JMSException {
        String src;
        String split_pat;
        String[] result;

        src = (String) expr.getArgument(0).evaluate(message_ctx);
        split_pat = (String) expr.getArgument(1).evaluate(message_ctx);

        if (expr.getNumArguments() > 2) {
            Integer limit;

            limit = (Integer) expr.getArgument(2).evaluate(message_ctx);

            result = src.split(split_pat, limit.intValue());
        } else {
            result = src.split(split_pat);
        }

        return java.util.Arrays.asList(result);
    }
}
