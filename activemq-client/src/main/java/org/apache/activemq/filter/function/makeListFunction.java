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
 * Filter function that creates a list with each argument being one element in the list.
 * For example:
 * <p/>
 * <p style="margin-left: 4em">
 * MAKELIST( '1', '2', '3' )
 * </p>
 */

public class makeListFunction implements FilterFunction {
    /**
     * Check whether the given expression is a valid call of this function.  Any number of arguments is accepted.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return true - if the expression is valid; false - otherwise.
     */

    public boolean isValid(FunctionCallExpression expr) {
        return true;
    }


    /**
     * Indicate that this function never evaluates to a Boolean result.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return false - this Filter Function never evaluates to a Boolean.
     */

    public boolean returnsBoolean(FunctionCallExpression expr) {
        return false;
    }


    /**
     * Evalutate the given expression, which consists of a call to this function, in the context given.  Creates
     * a list containing the evaluated results of its argument expressions.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @param    message - the context in which the call is being evaluated.
     * @return java.util.List - the result of the evaluation.
     */

    public Object evaluate(FunctionCallExpression expr, MessageEvaluationContext message)
            throws javax.jms.JMSException {
        java.util.ArrayList ele_arr;
        int num_arg;
        int cur;

        num_arg = expr.getNumArguments();
        ele_arr = new java.util.ArrayList(num_arg);

        cur = 0;
        while (cur < num_arg) {
            ele_arr.add(expr.getArgument(cur).evaluate(message));
            cur++;
        }

        return (java.util.List) ele_arr;
    }
}
