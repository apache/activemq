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
package org.apache.activemq.filter;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.List;

/**
 * A sequence of expressions, to be combined with OR or AND conjunctions.
 *
 */
public abstract class LogicExpression implements BooleanExpression {

    protected final List<BooleanExpression> expressions = new ArrayList<>(2);

    private LogicExpression(BooleanExpression lvalue, BooleanExpression rvalue) {
        expressions.add(lvalue);
        expressions.add(rvalue);
    }

    protected void addExpression(BooleanExpression expression) {
        expressions.add(expression);
    }

    public BooleanExpression getLeft() {
        if (expressions.size() == 2) {
            return expressions.get(0);
        }
        throw new IllegalStateException("This expression is not binary: " + this);
    }

    public BooleanExpression getRight() {
        if (expressions.size() == 2) {
            return expressions.get(1);
        }
        throw new IllegalStateException("This expression is not binary: " + this);
    }

    /**
     * Returns the symbol that represents this binary expression.  For example, addition is
     * represented by "+"
     *
     * @return
     */
    public abstract String getExpressionSymbol();

    @Override
    public String toString() {
        if (expressions.size() == 2) {
            return "( " + expressions.get(0) + " " + getExpressionSymbol() + " " + expressions.get(1) + " )";
        }
        StringBuilder result = new StringBuilder("(");
        int count = 0;
        for (BooleanExpression expression : expressions) {
            if (count++ > 0) {
                result.append(" " + getExpressionSymbol() + " ");
            }
            result.append(expression.toString());
        }
        result.append(")");
        return result.toString();
    }

    public static BooleanExpression createOR(BooleanExpression lvalue, BooleanExpression rvalue) {
        if (lvalue instanceof ORExpression) {
            ORExpression orExpression = (ORExpression) lvalue;
            orExpression.addExpression(rvalue);
            return orExpression;
        } else {
            return new ORExpression(lvalue, rvalue);
        }
    }

    public static BooleanExpression createAND(BooleanExpression lvalue, BooleanExpression rvalue) {
        if (lvalue instanceof ANDExpression) {
            ANDExpression orExpression = (ANDExpression) lvalue;
            orExpression.addExpression(rvalue);
            return orExpression;
        } else {
            return new ANDExpression(lvalue, rvalue);
        }
    }

    @Override
    public abstract Object evaluate(MessageEvaluationContext message) throws JMSException;

    @Override
    public abstract boolean matches(MessageEvaluationContext message) throws JMSException;

    public static class ORExpression extends LogicExpression {

        public ORExpression(BooleanExpression lvalue, BooleanExpression rvalue) {
            super(lvalue, rvalue);
        }

        @Override
        public Object evaluate(MessageEvaluationContext message) throws JMSException {
            boolean someNulls = false;
            for (BooleanExpression expression : expressions) {
                Boolean lv = (Boolean)expression.evaluate(message);
                if (lv != null && lv.booleanValue()) {
                    return Boolean.TRUE;
                }
                if (lv == null) {
                    someNulls = true;
                }
            }
            if (someNulls) {
                return null;
            }
            return Boolean.FALSE;
        }

        @Override
        public boolean matches(MessageEvaluationContext message) throws JMSException {
            for (BooleanExpression expression : expressions) {
                boolean lv = expression.matches(message);
                if (lv) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String getExpressionSymbol() {
            return "OR";
        }
    }

    private static class ANDExpression extends LogicExpression {

        public ANDExpression(BooleanExpression lvalue, BooleanExpression rvalue) {
            super(lvalue, rvalue);
        }

        @Override
        public Object evaluate(MessageEvaluationContext message) throws JMSException {
            boolean someNulls = false;
            for (BooleanExpression expression : expressions) {
                Boolean lv = (Boolean)expression.evaluate(message);
                if (lv != null && !lv.booleanValue()) {
                    return Boolean.FALSE;
                }
                if (lv == null) {
                    someNulls = true;
                }
            }
            if (someNulls) {
                return null;
            }
            return Boolean.TRUE;
        }

        @Override
        public boolean matches(MessageEvaluationContext message) throws JMSException {
            for (BooleanExpression expression : expressions) {
                boolean lv = expression.matches(message);
                if (!lv) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String getExpressionSymbol() {
            return "AND";
        }
    }
}
