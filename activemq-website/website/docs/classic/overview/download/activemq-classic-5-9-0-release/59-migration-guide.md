---
title: "5.9 Migration Guide"
slug: /59-migration-guide
---

There are some changes in 5.9 that may require user intervention

1.  Schema change to JDBC Persistence Adapter. XID column type change.
    *   If you use XA transactions you need to ensure there are no XA transactions pending completion before you upgrade. Some mode detail in [AMQ-4628](https://issues.apache.org/jira/browse/AMQ-4628)