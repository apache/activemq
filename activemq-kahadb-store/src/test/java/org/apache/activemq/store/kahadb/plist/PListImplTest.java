package org.apache.activemq.store.kahadb.plist;

import org.apache.activemq.store.PListStore;
import org.apache.activemq.store.PListTestSupport;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PListImplTest extends PListTestSupport {


    @Override
    protected PListStoreImpl createPListStore() {
        return new PListStoreImpl();
    }

    protected PListStore createConcurrentAddIteratePListStore() {
        PListStoreImpl store = createPListStore();
        store.setIndexPageSize(2 * 1024);
        store.setJournalMaxFileLength(1024 * 1024);
        store.setCleanupInterval(-1);
        store.setIndexEnablePageCaching(false);
        store.setIndexWriteBatchSize(100);
        return store;
    }

    @Override
    protected PListStore createConcurrentAddRemovePListStore() {
        PListStoreImpl store = createPListStore();
        store.setCleanupInterval(400);
        store.setJournalMaxFileLength(1024*5);
        store.setLazyInit(false);
        return store;
    }

    @Override
    protected PListStore createConcurrentAddRemoveWithPreloadPListStore() {
        PListStoreImpl store = createPListStore();
        store.setJournalMaxFileLength(1024*5);
        store.setCleanupInterval(5000);
        store.setIndexWriteBatchSize(500);
        return store;
    }

    @Override
    protected PListStore createConcurrentAddIterateRemovePListStore(boolean enablePageCache) {
        PListStoreImpl store = createPListStore();
        store.setIndexEnablePageCaching(enablePageCache);
        store.setIndexPageSize(2*1024);
        return store;
    }
}
