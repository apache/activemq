**风险等级**: 🟢 低（bindings 内部控制）

**文件**: [`ReadOnlyContext.lookup()`](activemq-client/src/main/java/org/apache/activemq/jndi/ReadOnlyContext.java:207)

**漏洞描述**:

`ReadOnlyContext.lookup()` 支持 `LinkRef` 递归查找和 `Reference` 对象的 `NamingManager.getObjectInstance()` 调用：

```java
if (result instanceof LinkRef) {
    LinkRef ref = (LinkRef)result;
    result = lookup(ref.getLinkName());  // 递归查找
}
if (result instanceof Reference) {
    result = NamingManager.getObjectInstance(result, null, null, this.environment);  // 触发 ObjectFactory
}
```

**利用条件**:
- 攻击者能控制 `ReadOnlyContext` 的 bindings 内容
- bindings 由 `ActiveMQInitialContextFactory` 内部控制，通常不可被外部操纵

**缓解措施**: 确保 `ReadOnlyContext` 的 bindings 不接受外部输入。
