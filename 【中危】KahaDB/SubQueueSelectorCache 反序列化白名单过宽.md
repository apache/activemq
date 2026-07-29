**风险等级**: 🟡 中（需要文件系统写入权限）

**文件**:
- [`MessageDatabase.MessageDatabaseObjectInputStream`](activemq-kahadb-store/src/main/java/org/apache/activemq/store/kahadb/MessageDatabase.java:4122)
- [`SubQueueSelectorCacheBroker.SubSelectorClassObjectInputStream`](activemq-broker/src/main/java/org/apache/activemq/plugin/SubQueueSelectorCacheBroker.java:364)

**漏洞描述**:

KahaDB 和 SubQueueSelectorCache 的反序列化白名单允许 `com.thoughtworks.xstream` 包：

```java
protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
    if (!(desc.getName().startsWith("java.lang.")
            || desc.getName().startsWith("com.thoughtworks.xstream")  // 允许 XStream 包
            || desc.getName().startsWith("java.util.")
            || desc.getName().length() > 2 && desc.getName().substring(2).startsWith("java.util.")
            || desc.getName().startsWith("org.apache.activemq."))) {
        throw new InvalidClassException("Unauthorized deserialization attempt", desc.getName());
    }
    return super.resolveClass(desc);
}
```

**风险分析**:
- `com.thoughtworks.xstream` 包中可能包含可利用的 gadget 类
- 如果攻击者能写入 KahaDB 存储文件或 SubQueueSelectorCache 文件，可触发反序列化
- 需要文件系统写入权限，降低了实际风险

**利用条件**:
- 攻击者能写入 KahaDB 存储文件（`db.data`）或 SubQueueSelectorCache 文件
- classpath 中存在可利用的 XStream gadget 链

**缓解措施**: 收紧白名单，移除 `com.thoughtworks.xstream` 包，或使用更精确的类名匹配。

--- 如果审核通过，请帮我申请CVE，CVE编号:360漏洞研究院-rockmelodies
