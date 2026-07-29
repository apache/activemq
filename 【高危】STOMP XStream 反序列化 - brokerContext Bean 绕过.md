【高危】STOMP XStream 反序列化 - brokerContext Bean 绕过
### 【高危】STOMP XStream 反序列化 - brokerContext Bean 绕过

**风险等级**: 🔴 高（条件性 RCE）

**文件**: [`JmsFrameTranslator.createXStream()`](activemq-stomp/src/main/java/org/apache/activemq/transport/stomp/JmsFrameTranslator.java:252)

**漏洞描述**:

STOMP 协议的 `JmsFrameTranslator.createXStream()` 方法存在安全绕过风险。当 `brokerContext` 不为 null 时，方法会从 Spring `ApplicationContext` 中获取 `XStream` bean：

```java
protected XStream createXStream() {
    XStream xstream = null;
    if (brokerContext != null) {
        Map<String, XStream> beans = brokerContext.getBeansOfType(XStream.class);
        for (XStream bean : beans.values()) {
            if (bean != null) {
                xstream = bean;  // 直接使用 Spring 中的 XStream bean
                break;
            }
        }
    }
    if (xstream == null) {
        xstream = XStreamSupport.createXStream();  // 安全配置的 fallback
        xstream.ignoreUnknownElements();
    }
    // ...
    return xstream;
}
```

**攻击链**:

1. 攻击者通过 STOMP 协议发送消息，设置 `transformation` header 为 `jms-object-xml`
2. [`ProtocolConverter.convertMessage()`](activemq-stomp/src/main/java/org/apache/activemq/transport/stomp/ProtocolConverter.java:908) 调用 `findTranslator()` 加载 `JmsFrameTranslator`
3. [`JmsFrameTranslator.convertFrame()`](activemq-stomp/src/main/java/org/apache/activemq/transport/stomp/JmsFrameTranslator.java:67) 根据 `transformation=jms-object-xml` 调用 `createObjectMessage(in)`
4. [`createObjectMessage()`](activemq-stomp/src/main/java/org/apache/activemq/transport/stomp/JmsFrameTranslator.java:196) 调用 `getXStream().unmarshal(in)` 反序列化客户端提供的 XML

**利用条件**:
- Spring 配置中定义了 `XStream` bean（且未配置安全白名单）
- 攻击者可访问 STOMP 端口

**测试代码佐证**: [`StompTest.java`](activemq-stomp/src/test/java/org/apache/activemq/transport/stomp/StompTest.java:1309) 中明确测试了 ProcessBuilder RCE：

```java
frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" +
        "transformation:" + Stomp.Transformations.JMS_OBJECT_XML + "\n\n" +
        "<java.lang.ProcessBuilder><command><string>id</string></command></java.lang.ProcessBuilder>" + Stomp.NULL;
```

**缓解措施**: 确保 Spring 配置中不定义未配置安全白名单的 `XStream` bean，或修改 `createXStream()` 始终使用 `XStreamSupport.createXStream()`。

如果审核通过，请帮我申请个CVE，CVE作者写 360漏洞研究院-rockmelodies,谢谢
