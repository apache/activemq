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
package org.apache.activemq.store.kahadb.disk.page;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.activemq.store.kahadb.disk.page.PageFile.PageFileCompactionStrategy;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PageFileCompactionTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private PageFile pf;

  @After
  public void tearDown() throws Exception {
    if (pf != null) {
      try {
        pf.unload();
      } catch (Exception ignored) {
          // ignore
      }
    }
  }

  // min .1
  // max .3
  @Test
  public void testFreeAll() throws IOException {
    pf = new PageFile(tempFolder.newFolder(), "pagefile");
    pf.setCompactionStrategy(PageFileCompactionStrategy.TRUNCATION);
    pf.load();

    // write 100 pages with some test data
    writePages(pf, 100, true);

    // verify page file properly allocated for all 100 pages
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    // compaction shouldn't do anything
    pf.compact();

    // file length shouldn't have changed
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    // free the first 50 pages, so 50% will now be marked as free
    freePages(pf, 0, 50, false);

    // Compaction is not able to truncate because the free pages
    // are only at the front
    pf.compact();

    // file length shouldn't have changed
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    // free the remaining pages, but do not flush
    freePages(pf, 50, 100, false);
    // compaction should not do anything becuase the free pages
    // were not yet flushed
    pf.compact();

    // file length shouldn't have changed
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    // now we flush, compaction should finally truncate
    pf.flush();
    pf.compact();

    // All 100 pages were free, 10 should be left because of the 10% min ratio
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(10), pf.getDiskSize());
    assertEquals(10, pf.getFreePageCount());
  }

  @Test
  public void testUnCleanShutdownRecoveryFile() throws Exception {
    var tmp = tempFolder.newFolder();
    pf = new PageFile(tmp, "pagefile");
    pf.setCompactionStrategy(PageFileCompactionStrategy.TRUNCATION);
    pf.load();

    // write 100 pages with test data
    writePages(pf, 100, true);
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    // free 30 pages (the threshold) so we compact
    freePages(pf, 70, 100, true);
    assertEquals(30, pf.getFreePageCount());
    assertEquals(100, pf.getPageCount());

    pf.compact();

    // we should have truncated 20 pages
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(80), pf.getDiskSize());
    assertEquals(10, pf.getFreePageCount());
    assertEquals(80, pf.getPageCount());

    // At this point the PageFile was not closed and shut down properly
    // so the free page list wasn't persisted, etc.

    // The recovery file contains the last batch of writes if enabled, so
    // in this case the last thing written was to free 30 pages so
    // loading will bring back and reallocate the 30 free pages bringing
    // the file back to 100
    PageFile pf2 = new PageFile(tmp, "pagefile");
    pf2.load();
    // free page recovery is async
    assertTrue(Wait.waitFor(() -> pf2.recoveredFreeList.get() != null, 1000, 1));

    // free page pages won't be recovered until we flush so all 100
    // pages should still be in use
    assertEquals(0, pf2.getFreePageCount());
    assertEquals(100, pf2.getPageCount());
    assertEquals(pf2.toOffset(100), pf2.getDiskSize());

    // flush merges recovered free pages and writes them again
    pf2.flush();

    // restored back to before compaction
    assertEquals(pf2.getFile().length(), pf2.getDiskSize());
    assertEquals(pf2.toOffset(100), pf2.getDiskSize());
    assertEquals(30, pf2.getFreePageCount());
    assertEquals(100, pf2.getPageCount());
  }

  @Test
  public void testUnCleanShutdownNoRecoveryFile() throws Exception {
    var tmp = tempFolder.newFolder();
    pf = new PageFile(tmp, "pagefile");
    pf.setCompactionStrategy(PageFileCompactionStrategy.TRUNCATION);
    // disable recovery file
    pf.setEnableRecoveryFile(false);
    pf.load();

    // write 100 pages with test data
    writePages(pf, 100, true);
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    // free 30 pages (the threshold) so we compact
    freePages(pf, 70, 100, true);
    assertEquals(30, pf.getFreePageCount());
    assertEquals(100, pf.getPageCount());

    pf.compact();

    // we should have truncated 20 pages
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(80), pf.getDiskSize());
    assertEquals(10, pf.getFreePageCount());
    assertEquals(80, pf.getPageCount());

    // At this point the PageFile was not closed and shut down properly
    // so the free page list wasn't persisted, etc. On load
    // 80 pages come back (none free) because we truncated 20

    // The recovery file is not active
    PageFile pf2 = new PageFile(tmp, "pagefile");
    pf2.load();
    pf2.flush();

    // restored back to before compaction - the 20 pages were truncated
    // and won't come back and are lost without a recovery file
    assertEquals(pf2.getFile().length(), pf2.getDiskSize());
    assertEquals(pf2.toOffset(80), pf2.getDiskSize());
    assertEquals(0, pf2.getFreePageCount());
    assertEquals(80, pf2.getPageCount());



    // TODO we also want to test with recovery file still enabled, but
    // writing something after compaction and then unclean shutdown
    // In that case the recovery file would contain the new writes
    // and not the free pages so the compaction should hopefully hold
    // Ie if we compacted from 30 to 10 free pages, then write 1 page
    // and unclean shutdown, we would recover with 9 free pages left
    // as it would replay the 1 new write and not recover the previous
    // free pages.

//    assertEquals(pf2.getFile().length(), pf2.getDiskSize());
//    assertEquals(pf2.toOffset(80), pf2.getDiskSize());
//    assertEquals(10, pf2.getFreePageCount());
//    assertEquals(80, pf2.getPageCount());

  }

  @Test
  public void testMinMaxFreePageRatio() throws IOException {
    pf = new PageFile(tempFolder.newFolder(), "pagefile");
    pf.setCompactionStrategy(PageFileCompactionStrategy.TRUNCATION);
    // keep 15% when compacting
    pf.setMinFreePageCompactionRatio(.15f);
    // don't compact until we hit 40% free
    pf.setMaxFreePageCompactionRatio(.4f);
    pf.load();

    // write 100 pages of test data
    writePages(pf, 100, true);

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    // free 39 pages, 1 less than the threshold
    freePages(pf, 61, 100, true);
    pf.compact();

    // No compaction should have happened
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());
    assertEquals(39, pf.getFreePageCount());
    assertEquals(100, pf.getPageCount());

    // free 1 more page, we should now be at the 30% marker
    freePages(pf, 60, 61, true);
    pf.compact();

    // We keep 15% of the total size (so 15 pages) which means
    // 25 pages get truncated
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(75), pf.getDiskSize());
    assertEquals(15, pf.getFreePageCount());
    assertEquals(75, pf.getPageCount());

  }

  @Test
  public void testNoMinMax() throws IOException {
    pf = new PageFile(tempFolder.newFolder(), "pagefile");
    pf.setCompactionStrategy(PageFileCompactionStrategy.TRUNCATION);
    // Setting min to 0 means there is no minimum free pages to retain
    // so we will drop them all
    pf.setMinFreePageCompactionRatio(0);
    // Setting max to 0 means there's no threshold to start truncation so
    // it will try and compact as soon as possible
    pf.setMaxFreePageCompactionRatio(0);
    pf.load();

    // allocate 50 pages with data, and 50 free
    writePages(pf, 50, true);
    allocateFree(pf, 50, true);

    assertEquals(100, pf.getPageCount());
    assertEquals(50, pf.getFreePageCount());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    // this should truncate all 50 free pages
    pf.compact();

    assertEquals(50, pf.getPageCount());
    assertEquals(0, pf.getFreePageCount());
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(50), pf.getDiskSize());

    // all should be removed now
    freePages(pf, 0, 50, true);
    pf.compact();

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(0), pf.getDiskSize());
  }

  @Test
  public void testNever() throws IOException {
    // Default is to never compact
    pf = new PageFile(tempFolder.newFolder(), "pagefile");
    pf.load();

    // allocate all free pages
    allocateFree(pf, 100, true);

    assertEquals(100, pf.getFreePageCount());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    pf.compact();

    // compaction is disabled, should not compact
    assertEquals(100, pf.getFreePageCount());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    // enable but set min/max to 100 so will never compact
    pf.unload();
    pf.setCompactionStrategy(PageFileCompactionStrategy.TRUNCATION);
    pf.setMinFreePageCompactionRatio(1);
    pf.setMaxFreePageCompactionRatio(1);
    pf.load();
    pf.compact();

    assertEquals(100, pf.getFreePageCount());
    assertEquals(pf.toOffset(100), pf.getDiskSize());
  }

  @Test
  public void testConfigValidation() throws IOException {
    // Default is to never compact
    pf = new PageFile(tempFolder.newFolder(), "pagefile");

    assertThrows(NullPointerException.class, () -> pf.setCompactionStrategy(null));
    assertThrows(IllegalArgumentException.class, () -> pf.setMinFreePageCompactionRatio(-.01f));
    assertThrows(IllegalArgumentException.class, () -> pf.setMinFreePageCompactionRatio(1.00001f));
    assertThrows(IllegalArgumentException.class, () -> pf.setMaxFreePageCompactionRatio(-.01f));
    assertThrows(IllegalArgumentException.class, () -> pf.setMaxFreePageCompactionRatio(1.00001f));

    // should be no errors
    pf.setMinFreePageCompactionRatio(0f);
    pf.setMinFreePageCompactionRatio(1f);
    pf.setMaxFreePageCompactionRatio(0f);
    pf.setMaxFreePageCompactionRatio(1f);

    // max can't be less than min
    pf.setMaxFreePageCompactionRatio(.2f);
    pf.setMinFreePageCompactionRatio(.3f);
    assertThrows(IllegalStateException.class, () -> pf.load());
  }


  private void allocateFree(PageFile pf, int pageCount, boolean flush) throws IOException {
    // Insert some data into the page file.
    Transaction tx = pf.tx();
    for (int i = 0; i < pageCount; i++) {
      Page<String> page = tx.allocate();
      assertEquals(Page.PAGE_FREE_TYPE, page.getType());
    }
    tx.commit();
    if (flush) {
      pf.flush();
    }
  }

  private void writePages(PageFile pf, int pageCount, boolean flush) throws IOException {
    // Insert some data into the page file.
    Transaction tx = pf.tx();
    for (int i = 0; i < pageCount; i++) {
      Page<String> page = tx.allocate();
      assertEquals(Page.PAGE_FREE_TYPE, page.getType());
      page.set("page:" + i);
      tx.store(page, StringMarshaller.INSTANCE, false);
    }
    tx.commit();
    if (flush) {
      pf.flush();
    }
  }

  private void freePages(PageFile pf, int start, int end, boolean flush) throws IOException {
    Transaction tx = pf.tx();
    for (int i = start; i < end; i++) {
      tx.free(i);
    }
    tx.commit();
    if (flush) {
      pf.flush();
    }
  }
}
