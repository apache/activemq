/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.tool.spi;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.ConnectionFactory;
import java.util.Properties;

public class ActiveMQPojoSPI implements SPIConnectionFactory {
    public static final String KEY_BROKER_URL        = "factory.brokerUrl";
    public static final String KEY_USERNAME          = "factory.username";
    public static final String KEY_PASSWORD          = "factory.password";
    public static final String KEY_CLIENT_ID         = "factory.clientID";

    public static final String KEY_ASYNC_SEND        = "factory.asyncSend";
    public static final String KEY_ASYNC_DISPATCH    = "factory.asyncDispatch";
    public static final String KEY_ASYNC_SESSION     = "factory.asyncSession";
    public static final String KEY_CLOSE_TIMEOUT     = "factory.closeTimeout";
    public static final String KEY_COPY_MSG_ON_SEND  = "factory.copyMsgOnSend";
    public static final String KEY_DISABLE_TIMESTAMP = "factory.disableTimestamp";
    public static final String KEY_DEFER_OBJ_SERIAL  = "factory.deferObjSerial";
    public static final String KEY_ON_SEND_PREP_MSG  = "factory.onSendPrepMsg";
    public static final String KEY_OPTIM_ACK         = "factory.optimAck";
    public static final String KEY_OPTIM_DISPATCH    = "factory.optimDispatch";
    public static final String KEY_PREFETCH_QUEUE    = "factory.prefetchQueue";
    public static final String KEY_PREFETCH_TOPIC    = "factory.prefetchTopic";
    public static final String KEY_USE_COMPRESSION   = "factory.useCompression";
    public static final String KEY_USE_RETROACTIVE   = "factory.useRetroactive";

    public ConnectionFactory createConnectionFactory(Properties settings) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        configureConnectionFactory(factory, settings);
        return factory;
    }

    public void configureConnectionFactory(ConnectionFactory jmsFactory, Properties settings) throws Exception {
        ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory)jmsFactory;
        String setting;

        setting = settings.getProperty(KEY_BROKER_URL);
        if (setting != null && setting.length() > 0) {
            factory.setBrokerURL(setting);
        }

        setting = settings.getProperty(KEY_USERNAME);
        if (setting != null && setting.length() > 0) {
            factory.setUserName(setting);
        }

        setting = settings.getProperty(KEY_PASSWORD);
        if (setting != null && setting.length() > 0) {
            factory.setPassword(setting);
        }

        setting = settings.getProperty(KEY_CLIENT_ID);
        if (setting != null && setting.length() > 0) {
            factory.setClientID(setting);
        }

        setting = settings.getProperty(KEY_ASYNC_SEND);
        if (setting != null && setting.length() > 0) {
            factory.setUseAsyncSend(Boolean.getBoolean(setting));
        }

        setting = settings.getProperty(KEY_ASYNC_DISPATCH);
        if (setting != null && setting.length() > 0) {
            factory.setAsyncDispatch(Boolean.getBoolean(setting));
        }

        setting = settings.getProperty(KEY_ASYNC_SESSION);
        if (setting != null && setting.length() > 0) {
            factory.setAlwaysSessionAsync(Boolean.getBoolean(setting));
        }

        setting = settings.getProperty(KEY_CLOSE_TIMEOUT);
        if (setting != null && setting.length() > 0) {
            factory.setCloseTimeout(Integer.parseInt(setting));
        }

        setting = settings.getProperty(KEY_COPY_MSG_ON_SEND);
        if (setting != null && setting.length() > 0) {
            factory.setCopyMessageOnSend(Boolean.getBoolean(setting));
        }

        setting = settings.getProperty(KEY_DISABLE_TIMESTAMP);
        if (setting != null && setting.length() > 0) {
            factory.setDisableTimeStampsByDefault(Boolean.getBoolean(setting));
        }

        setting = settings.getProperty(KEY_DEFER_OBJ_SERIAL);
        if (setting != null && setting.length() > 0) {
            factory.setObjectMessageSerializationDefered(Boolean.getBoolean(setting));
        }

        setting = settings.getProperty(KEY_ON_SEND_PREP_MSG);
        if (setting != null && setting.length() > 0) {
            factory.setOnSendPrepareMessageBody(Boolean.getBoolean(setting));
        }

        setting = settings.getProperty(KEY_OPTIM_ACK);
        if (setting != null && setting.length() > 0) {
            factory.setOptimizeAcknowledge(Boolean.getBoolean(setting));
        }

        setting = settings.getProperty(KEY_OPTIM_DISPATCH);
        if (setting != null && setting.length() > 0) {
            factory.setOptimizedMessageDispatch(Boolean.getBoolean(setting));
        }

        setting = settings.getProperty(KEY_PREFETCH_QUEUE);
        if (setting != null && setting.length() > 0) {
            factory.getPrefetchPolicy().setQueuePrefetch(Integer.parseInt(setting));
        }

        setting = settings.getProperty(KEY_PREFETCH_TOPIC);
        if (setting != null && setting.length() > 0) {
            factory.getPrefetchPolicy().setTopicPrefetch(Integer.parseInt(setting));
        }

        setting = settings.getProperty(KEY_USE_COMPRESSION);
        if (setting != null && setting.length() > 0) {
            factory.setUseCompression(Boolean.getBoolean(setting));
        }

        setting = settings.getProperty(KEY_USE_RETROACTIVE);
        if (setting != null && setting.length() > 0) {
            factory.setUseRetroactiveConsumer(Boolean.getBoolean(setting));
        }
    }
}
