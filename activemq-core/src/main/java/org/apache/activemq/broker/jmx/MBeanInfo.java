package org.apache.activemq.broker.jmx;

import static java.lang.annotation.ElementType.*;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(value=RetentionPolicy.RUNTIME)
@Target({CONSTRUCTOR, METHOD, PARAMETER, TYPE, FIELD})
public @interface MBeanInfo {
    String value();
}
