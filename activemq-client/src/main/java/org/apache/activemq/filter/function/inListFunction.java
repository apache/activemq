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
 * Filter function that matches a value against a list of values and evaluates to an indicator of membership in the
 * list.  For example:
 * <p/>
 * <p style="margin-left: 4em">
 * INLIST( SPLIT('1,2,3', ',') , '2' )
 * </p>
 * <p/>
 * Note that the first argument must be a List.  Strings containing lists are not acceptable; for example,
 * INLIST('1,2,3', '1'), will cause an exception to be thrown at evaluation-time.
 */

public class inListFunction implements FilterFunction {
    /**
     * Check whether the given expression is a valid call of this function.  Two arguments are required.  Note that
     * the evaluated results of the arguments will be compared with Object#equals().
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return true - if the expression is valid; false - otherwise.
     */

    public boolean isValid(FunctionCallExpression expr) {
        if (expr.getNumArguments() != 2)
            return false;

        return true;
    }


    /**
     * Check whether the given expression, which consists of a call to this function, evaluates to a Boolean.
     * If the function can return different more than one type of value at evaluation-time, it must decide whether
     * to cast the result to a Boolean at this time.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return true - if the expression is valid; false - otherwise.
     */

    public boolean returnsBoolean(FunctionCallExpression expr) {
        return true;
    }


    /**
     * Evalutate the given expression, which consists of a call to this function, in the context given.  Checks
     * whether the second argument is a member of the list in the first argument.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @param    message_ctx - the context in which the call is being evaluated.
     * @return Boolean - the result of the evaluation.
     */

    public Object evaluate(FunctionCallExpression expr, MessageEvaluationContext message_ctx)
            throws javax.jms.JMSException {
        java.util.List arr;
        int cur;
        Object cand;
        boolean found_f;

        arr = (java.util.List) expr.getArgument(0).evaluate(message_ctx);
        cand = expr.getArgument(1).evaluate(message_ctx);

        cur = 0;
        found_f = false;
        while ((cur < arr.size()) && (!found_f)) {
            found_f = arr.get(cur).equals(cand);
            cur++;
        }

        return Boolean.valueOf(found_f);
    }
}
