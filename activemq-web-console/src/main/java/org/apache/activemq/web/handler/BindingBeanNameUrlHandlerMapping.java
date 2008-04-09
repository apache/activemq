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
package org.apache.activemq.web.handler;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.ServletRequestDataBinder;
import org.springframework.web.servlet.handler.BeanNameUrlHandlerMapping;
import org.springframework.web.servlet.HandlerExecutionChain;

/**
 * @version $Revision$
 */
public class BindingBeanNameUrlHandlerMapping extends BeanNameUrlHandlerMapping {
    private static final transient Log LOG = LogFactory.getLog(BindingBeanNameUrlHandlerMapping.class);

    protected Object getHandlerInternal(HttpServletRequest request) throws Exception {
        Object object = super.getHandlerInternal(request);

        if (object instanceof String) {
            String handlerName = (String) object;
            object = getApplicationContext().getBean(handlerName);
        }
        if (object instanceof HandlerExecutionChain) {
            HandlerExecutionChain handlerExecutionChain = (HandlerExecutionChain) object;
            object = handlerExecutionChain.getHandler();
        }

        if (object != null) {
            ServletRequestDataBinder binder = new ServletRequestDataBinder(object, "request");
            try {
                binder.bind(request);
                binder.setIgnoreUnknownFields(true);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Bound POJO is now: " + object);
                }
            }
            catch (Exception e) {
                LOG.warn("Caught: " + e, e);
                throw e;
            }
        }
        return object;
    }
}
