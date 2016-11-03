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
package org.apache.activemq.util;

import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.command.ActiveMQDestination;
import org.fusesource.hawtbuf.UTF8Buffer;

/**
 * Type conversion support for ActiveMQ.
 */
public final class TypeConversionSupport {

    private static final Converter IDENTITY_CONVERTER = new Converter() {
        @Override
        public Object convert(Object value) {
            return value;
        }
    };

    private static class ConversionKey {
        final Class<?> from;
        final Class<?> to;
        final int hashCode;

        public ConversionKey(Class<?> from, Class<?> to) {
            this.from = from;
            this.to = to;
            this.hashCode = from.hashCode() ^ (to.hashCode() << 1);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ConversionKey other = (ConversionKey) obj;
            if (from == null) {
                if (other.from != null)
                    return false;
            } else if (!from.equals(other.from))
                return false;
            if (to == null) {
                if (other.to != null)
                    return false;
            } else if (!to.equals(other.to))
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    public interface Converter {
        Object convert(Object value);
    }

    private static final Map<ConversionKey, Converter> CONVERSION_MAP = new HashMap<ConversionKey, Converter>();
    static {
        Converter toStringConverter = new Converter() {
            @Override
            public Object convert(Object value) {
                return value.toString();
            }
        };
        CONVERSION_MAP.put(new ConversionKey(Boolean.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Byte.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Integer.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Long.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Float.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Double.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(UTF8Buffer.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(URI.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(BigInteger.class, String.class), toStringConverter);

        CONVERSION_MAP.put(new ConversionKey(String.class, Boolean.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Boolean.valueOf((String)value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Byte.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Byte.valueOf((String)value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Short.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Short.valueOf((String)value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Integer.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Integer.valueOf((String)value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Long.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Long.valueOf((String)value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Float.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Float.valueOf((String)value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Double.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Double.valueOf((String)value);
            }
        });

        Converter longConverter = new Converter() {
            @Override
            public Object convert(Object value) {
                return Long.valueOf(((Number)value).longValue());
            }
        };
        CONVERSION_MAP.put(new ConversionKey(Byte.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Integer.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Date.class, Long.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Long.valueOf(((Date)value).getTime());
            }
        });

        Converter intConverter = new Converter() {
            @Override
            public Object convert(Object value) {
                return Integer.valueOf(((Number)value).intValue());
            }
        };
        CONVERSION_MAP.put(new ConversionKey(Byte.class, Integer.class), intConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, Integer.class), intConverter);

        CONVERSION_MAP.put(new ConversionKey(Byte.class, Short.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Short.valueOf(((Number)value).shortValue());
            }
        });

        CONVERSION_MAP.put(new ConversionKey(Float.class, Double.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return new Double(((Number)value).doubleValue());
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, ActiveMQDestination.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return ActiveMQDestination.createDestination((String)value, ActiveMQDestination.QUEUE_TYPE);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, URI.class), new Converter() {
            @Override
            public Object convert(Object value) {
                String text = value.toString();
                try {
                    return new URI(text);
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private TypeConversionSupport() {
    }

    public static Object convert(Object value, Class<?> to) {
        if (value == null) {
            // lets avoid NullPointerException when converting to boolean for null values
            if (boolean.class.isAssignableFrom(to)) {
                return Boolean.FALSE;
            }
            return null;
        }

        // eager same instance type test to avoid the overhead of invoking the type converter
        // if already same type
        if (to.isInstance(value)) {
            return to.cast(value);
        }

        // lookup converter
        Converter c = lookupConverter(value.getClass(), to);
        if (c != null) {
            return c.convert(value);
        } else {
            return null;
        }
    }

    public static Converter lookupConverter(Class<?> from, Class<?> to) {
        // use wrapped type for primitives
        if (from.isPrimitive()) {
            from = convertPrimitiveTypeToWrapperType(from);
        }
        if (to.isPrimitive()) {
            to = convertPrimitiveTypeToWrapperType(to);
        }

        if (from.equals(to)) {
            return IDENTITY_CONVERTER;
        }

        return CONVERSION_MAP.get(new ConversionKey(from, to));
    }

    /**
     * Converts primitive types such as int to its wrapper type like
     * {@link Integer}
     */
    private static Class<?> convertPrimitiveTypeToWrapperType(Class<?> type) {
        Class<?> rc = type;
        if (type.isPrimitive()) {
            if (type == int.class) {
                rc = Integer.class;
            } else if (type == long.class) {
                rc = Long.class;
            } else if (type == double.class) {
                rc = Double.class;
            } else if (type == float.class) {
                rc = Float.class;
            } else if (type == short.class) {
                rc = Short.class;
            } else if (type == byte.class) {
                rc = Byte.class;
            } else if (type == boolean.class) {
                rc = Boolean.class;
            }
        }
        return rc;
    }
}
