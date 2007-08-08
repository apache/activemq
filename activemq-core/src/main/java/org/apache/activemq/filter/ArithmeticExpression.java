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

/**
 * An expression which performs an operation on two expression values
 * 
 * @version $Revision: 1.2 $
 */
public abstract class ArithmeticExpression extends BinaryExpression {

    protected static final int INTEGER = 1;
    protected static final int LONG = 2;
    protected static final int DOUBLE = 3;

    /**
     * @param left
     * @param right
     */
    public ArithmeticExpression(Expression left, Expression right) {
        super(left, right);
    }

    public static Expression createPlus(Expression left, Expression right) {
        return new ArithmeticExpression(left, right) {
            protected Object evaluate(Object lvalue, Object rvalue) {
                if (lvalue instanceof String) {
                    String text = (String) lvalue;
                    String answer = text + rvalue;
                    return answer;
                }
                else if (lvalue instanceof Number) {
                    return plus((Number) lvalue, asNumber(rvalue));
                }
                throw new RuntimeException("Cannot call plus operation on: " + lvalue + " and: " + rvalue);
            }

            public String getExpressionSymbol() {
                return "+";
            }
        };
    }

    public static Expression createMinus(Expression left, Expression right) {
        return new ArithmeticExpression(left, right) {
            protected Object evaluate(Object lvalue, Object rvalue) {
                if (lvalue instanceof Number) {
                    return minus((Number) lvalue, asNumber(rvalue));
                }
                throw new RuntimeException("Cannot call minus operation on: " + lvalue + " and: " + rvalue);
            }

            public String getExpressionSymbol() {
                return "-";
            }
        };
    }

    public static Expression createMultiply(Expression left, Expression right) {
        return new ArithmeticExpression(left, right) {

            protected Object evaluate(Object lvalue, Object rvalue) {
                if (lvalue instanceof Number) {
                    return multiply((Number) lvalue, asNumber(rvalue));
                }
                throw new RuntimeException("Cannot call multiply operation on: " + lvalue + " and: " + rvalue);
            }

            public String getExpressionSymbol() {
                return "*";
            }
        };
    }

    public static Expression createDivide(Expression left, Expression right) {
        return new ArithmeticExpression(left, right) {

            protected Object evaluate(Object lvalue, Object rvalue) {
                if (lvalue instanceof Number) {
                    return divide((Number) lvalue, asNumber(rvalue));
                }
                throw new RuntimeException("Cannot call divide operation on: " + lvalue + " and: " + rvalue);
            }

            public String getExpressionSymbol() {
                return "/";
            }
        };
    }

    public static Expression createMod(Expression left, Expression right) {
        return new ArithmeticExpression(left, right) {

            protected Object evaluate(Object lvalue, Object rvalue) {
                if (lvalue instanceof Number) {
                    return mod((Number) lvalue, asNumber(rvalue));
                }
                throw new RuntimeException("Cannot call mod operation on: " + lvalue + " and: " + rvalue);
            }

            public String getExpressionSymbol() {
                return "%";
            }
        };
    }

    protected Number plus(Number left, Number right) {
        switch (numberType(left, right)) {
            case INTEGER:
                return new Integer(left.intValue() + right.intValue());
            case LONG:
                return new Long(left.longValue() + right.longValue());
            default:
                return new Double(left.doubleValue() + right.doubleValue());
        }
    }

    protected Number minus(Number left, Number right) {
        switch (numberType(left, right)) {
            case INTEGER:
                return new Integer(left.intValue() - right.intValue());
            case LONG:
                return new Long(left.longValue() - right.longValue());
            default:
                return new Double(left.doubleValue() - right.doubleValue());
        }
    }

    protected Number multiply(Number left, Number right) {
        switch (numberType(left, right)) {
            case INTEGER:
                return new Integer(left.intValue() * right.intValue());
            case LONG:
                return new Long(left.longValue() * right.longValue());
            default:
                return new Double(left.doubleValue() * right.doubleValue());
        }
    }

    protected Number divide(Number left, Number right) {
        return new Double(left.doubleValue() / right.doubleValue());
    }

    protected Number mod(Number left, Number right) {
        return new Double(left.doubleValue() % right.doubleValue());
    }

    private int numberType(Number left, Number right) {
        if (isDouble(left) || isDouble(right)) {
            return DOUBLE;
        }
        else if (left instanceof Long || right instanceof Long) {
            return LONG;
        }
        else {
            return INTEGER;
        }
    }

    private boolean isDouble(Number n) {
        return n instanceof Float || n instanceof Double;
    }

    protected Number asNumber(Object value) {
        if (value instanceof Number) {
            return (Number) value;
        }
        else {
            throw new RuntimeException("Cannot convert value: " + value + " into a number");
        }
    }

    public Object evaluate(MessageEvaluationContext message) throws JMSException {
        Object lvalue = left.evaluate(message);
        if (lvalue == null) {
            return null;
        }
        Object rvalue = right.evaluate(message);
        if (rvalue == null) {
            return null;
        }
        return evaluate(lvalue, rvalue);
    }


    /**
     * @param lvalue
     * @param rvalue
     * @return
     */
    abstract protected Object evaluate(Object lvalue, Object rvalue);

}
