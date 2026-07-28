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
package org.apache.activemq.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import jakarta.jms.JMSException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates the fix for the race condition between getText(),
 * storeContentAndClear() and copy() in ActiveMQTextMessage that could cause
 * the message body to become permanently null.
 * <p>
 * The race existed because those methods performed multi-step
 * read-modify-clear operations on two fields (text and content) without a
 * common monitor:
 * <p>
 *   Thread A (getText):              Thread B (storeContentAndClear):
 *     1. reads content -> non-null     1. storeContent(): content non-null,
 *     2. decodes content -> text          skips encoding text -> content
 *     3. setContent(null)              2. text = null
 *
 *   Result: content=null (cleared by A), text=null (cleared by B).
 *   The body was permanently lost.
 * <p>
 * Real-world trigger: on the broker side, a transport thread calls
 * beforeMarshall() -> storeContentAndClear() while concurrently an XPath
 * selector evaluator (JAXPXPathEvaluator) or JMX browse calls getText() on
 * the same Message instance, and the network bridge calls copy() on it. The
 * broker dispatches the same Message object to multiple consumers without
 * copying it.
 * <p>
 * The fix synchronizes the state transitions on the message instance
 * itself: getText() decodes+clears under {@code synchronized(this)} (with a
 * double-checked volatile read of text for the fast path),
 * storeContentAndClear() stores+clears under the same monitor, copy() takes
 * the monitor while snapshotting both fields, and content/text are volatile.
 * <p>
 * Note:
 * This issue is specific to ActiveMQTextMessage because it is the only message
 * type that will only store either the unmarshalled data (text) or the
 * bytes, but not both. Other message types do not clear the content after unmarshaling.
 * This means that if multiple threads try and do a conversion on a message like Map
 * or Object message, it may convert twice, but you won't get null. The other
 * message types only clear unmarshaled content at certain points in the broker
 * if reduceMemoryFootprint is true.
 * <p>
 * This test also runs the relevant race condition tests on other message types
 * to confirm they do not suffer from the same null body issue.
 */
@RunWith(Parameterized.class)
public class ActiveMQMessageContentRaceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQMessageContentRaceTest.class);

    private static final String TEST_TEXT = "Hello, World! This is a test message body.";
    private static final int ITERATIONS = 20_000;
    private final byte messageType;

    public ActiveMQMessageContentRaceTest(byte messageType) {
        this.messageType = messageType;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {ActiveMQTextMessage.DATA_STRUCTURE_TYPE},
                // Add other message types to verify they don't suffer
                // from the same race condition either
                {ActiveMQBytesMessage.DATA_STRUCTURE_TYPE},
                {ActiveMQStreamMessage.DATA_STRUCTURE_TYPE},
                {ActiveMQMapMessage.DATA_STRUCTURE_TYPE},
                {ActiveMQObjectMessage.DATA_STRUCTURE_TYPE},
        });
    }

    /**
     * Creates a message in the state it has on the broker after arriving and
     * being trimmed by reduceMemoryFootprint: content is the marshalled
     * bytes, text is null.
     */
    private ActiveMQMessage brokerStateMessage() throws Exception {
        final ActiveMQMessage msg;
        if (messageType == ActiveMQTextMessage.DATA_STRUCTURE_TYPE) {
            msg = new ActiveMQTextMessage();
            asText(msg).setText(TEST_TEXT);
        } else if (messageType == ActiveMQBytesMessage.DATA_STRUCTURE_TYPE) {
            msg = new ActiveMQBytesMessage();
            asBytes(msg).writeBytes(TEST_TEXT.getBytes());
        } else if (messageType == ActiveMQStreamMessage.DATA_STRUCTURE_TYPE) {
            msg = new ActiveMQStreamMessage();
            asStream(msg).writeBytes(TEST_TEXT.getBytes());
        } else if (messageType == ActiveMQMapMessage.DATA_STRUCTURE_TYPE) {
            msg = new ActiveMQMapMessage();
            asMap(msg).setString("test1", TEST_TEXT);
        } else if (messageType == ActiveMQObjectMessage.DATA_STRUCTURE_TYPE) {
            msg = new ActiveMQObjectMessage();
            asObject(msg).setObject(TEST_TEXT);
        } else {
            throw new IllegalArgumentException("Unsupported data structure type: " + messageType);
        }
        msg.storeContent();
        msg.clearUnMarshalledState();
        return msg;
    }

    /**
     * Control case: sequential getText() / storeContentAndClear() round
     * trips preserve the body.
     */
    @Test
    public void testSequentialGetTextAndStoreContentPreservesBody() throws Exception {
        ActiveMQMessage msg = brokerStateMessage();
        assertNotNull("content should be set after storeContent()", msg.getContent());

        // getText() decodes content -> text (and clears content)
        assertEquals(TEST_TEXT, decodeBody(msg));

        // storeContentAndClear() re-encodes text -> content, clears text
        msg.storeContentAndClear();
        assertNotNull("content should be set after storeContentAndClear()", msg.getContent());

        // getText() again recovers the body
        assertEquals(TEST_TEXT, decodeBody(msg));
    }

    /**
     * The original race: concurrent getText() and storeContentAndClear()
     * on the same instance. Before the fix this lost the body within a few
     * thousand iterations on any multi-core machine; with the state
     * transitions synchronized on the message instance, no iteration may
     * lose the body.
     */
    @Test(timeout = 120_000)
    public void testConcurrentGetTextAndStoreContentAndClearPreservesBody() throws Exception {
        final AtomicInteger nullBodyCount = new AtomicInteger(0);
        final AtomicReference<String> firstFailure = new AtomicReference<>();

        for (int i = 0; i < ITERATIONS; i++) {
            ActiveMQMessage msg = brokerStateMessage();

            CyclicBarrier barrier = new CyclicBarrier(2);

            // simulates XPath selector evaluation / JMX browse
            Thread readerThread = new Thread(() -> {
                try {
                    barrier.await();
                    decodeBody(msg);
                } catch (Exception e) {
                    // ignore
                }
            });

            // simulates the transport thread marshalling for dispatch
            Thread marshalThread = new Thread(() -> {
                try {
                    barrier.await();
                    msg.storeContentAndClear();
                } catch (Exception e) {
                    // ignore
                }
            });

            readerThread.start();
            marshalThread.start();
            readerThread.join();
            marshalThread.join();

            String recoveredText = null;
            try {
                recoveredText = decodeBody(msg);
            } catch (Exception e) {
                // getText might throw if content is corrupted
            }

            if (recoveredText == null) {
                int count = nullBodyCount.incrementAndGet();
                if (firstFailure.get() == null) {
                    firstFailure.set("Iteration " + i + ": text=" + rawBody(msg) +
                            ", content=" + msg.getContent());
                }
                if (count >= 10) {
                    break;
                }
            }
        }

        if (nullBodyCount.get() > 0) {
            fail("RACE CONDITION: " + nullBodyCount.get() + " messages had null body due to " +
                    "concurrent getText() and storeContentAndClear(). First failure: " +
                    firstFailure.get());
        }
        LOG.info("Body preserved across {} concurrent getText/storeContentAndClear iterations",
                ITERATIONS);
    }

    /**
     * Three-way race including copy(): the network bridge copies the same
     * Message instance that local dispatch is marshalling while a selector
     * reads it. Both the original and the copy must retain a recoverable
     * body.
     */
    @Test(timeout = 120_000)
    public void testConcurrentCopyGetTextAndStoreContentPreservesBody() throws Exception {
        final AtomicInteger failures = new AtomicInteger(0);
        final AtomicReference<String> firstFailure = new AtomicReference<>();

        for (int i = 0; i < ITERATIONS / 2; i++) {
            ActiveMQMessage msg = brokerStateMessage();
            AtomicReference<Message> copyRef = new AtomicReference<>();

            CyclicBarrier barrier = new CyclicBarrier(3);

            Thread readerThread = new Thread(() -> {
                try {
                    barrier.await();
                    decodeBody(msg);
                } catch (Exception e) {
                    // ignore
                }
            });
            Thread marshalThread = new Thread(() -> {
                try {
                    barrier.await();
                    msg.storeContentAndClear();
                } catch (Exception e) {
                    // ignore
                }
            });
            Thread copyThread = new Thread(() -> {
                try {
                    barrier.await();
                    copyRef.set(msg.copy());
                } catch (Exception e) {
                    // ignore
                }
            });

            readerThread.start();
            marshalThread.start();
            copyThread.start();
            readerThread.join();
            marshalThread.join();
            copyThread.join();

            String originalText = null;
            String copyText = null;
            try {
                originalText = decodeBody(msg);
                ActiveMQMessage copy = (ActiveMQMessage) copyRef.get();
                copyText = copy != null ? decodeBody(copy): "no-copy-made";
            } catch (Exception e) {
                // fall through with nulls
            }

            if (originalText == null || copyText == null) {
                int count = failures.incrementAndGet();
                if (firstFailure.get() == null) {
                    firstFailure.set("Iteration " + i + ": original=" + originalText +
                            ", copy=" + copyText);
                }
                if (count >= 10) {
                    break;
                }
            }
        }

        if (failures.get() > 0) {
            fail("RACE CONDITION: " + failures.get() + " iterations lost the body on the " +
                    "original or its copy() during concurrent copy/getText/storeContentAndClear. " +
                    "First failure: " + firstFailure.get());
        }
        LOG.info("Body preserved on original and copy across {} three-way iterations",
                ITERATIONS / 2);
    }

    /**
     * Deterministically pins the locking discipline of the fix: the state
     * transitions synchronize on the message instance itself. While another
     * thread holds the message monitor, storeContentAndClear() must block —
     * so the getText() decode+clear and the storeContentAndClear()
     * store+clear can never interleave.
     *
     * If the implementation ever moves off {@code synchronized(this)} (or
     * drops synchronization), this test fails and the concurrent tests above
     * become the (probabilistic) safety net.
     */
    @Test(timeout = 60_000)
    public void testStateTransitionsSynchronizeOnMessageInstance() throws Exception {
        // Only applies to text message
        Assume.assumeTrue(this.messageType == ActiveMQTextMessage.DATA_STRUCTURE_TYPE);

        ActiveMQMessage msg = brokerStateMessage();
        // decode so text is populated and storeContentAndClear has work to do
        assertEquals(TEST_TEXT, decodeBody(msg));

        CountDownLatch monitorHeld = new CountDownLatch(1);
        CountDownLatch releaseMonitor = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            synchronized (msg) {
                monitorHeld.countDown();
                try {
                    releaseMonitor.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        holder.start();
        assertTrue("holder should acquire the message monitor", monitorHeld.await(5, TimeUnit.SECONDS));

        CountDownLatch storeDone = new CountDownLatch(1);
        Thread storer = new Thread(() -> {
            msg.storeContentAndClear();
            storeDone.countDown();
        });
        storer.start();

        assertTrue("storeContentAndClear must BLOCK while the message monitor is held — " +
                "the fix serializes text/content state transitions on the message instance",
                !storeDone.await(500, TimeUnit.MILLISECONDS));

        releaseMonitor.countDown();
        assertTrue("storeContentAndClear should complete once the monitor is released",
                storeDone.await(5, TimeUnit.SECONDS));
        holder.join(5000);
        storer.join(5000);

        assertNotNull("content must be set after storeContentAndClear", msg.getContent());
        assertEquals("body must survive the round trip", TEST_TEXT, decodeBody(msg));
    }

    private String decodeBody(final ActiveMQMessage msg) throws JMSException {
        if (messageType == ActiveMQTextMessage.DATA_STRUCTURE_TYPE) {
            return asText(msg).getText();
        } else if (messageType == ActiveMQBytesMessage.DATA_STRUCTURE_TYPE) {
            var bytesMsg = asBytes(msg);
            bytesMsg.setReadOnlyBody(true);
            bytesMsg.reset();
            byte[] bytes = new byte[(int) bytesMsg.getBodyLength()];
            bytesMsg.readBytes(bytes);
            return new String(bytes);
        } else if (messageType == ActiveMQStreamMessage.DATA_STRUCTURE_TYPE) {
            var bytesMsg = asStream(msg);
            bytesMsg.setReadOnlyBody(true);
            bytesMsg.reset();
            byte[] bytes = new byte[TEST_TEXT.length()];
            bytesMsg.readBytes(bytes);
            return new String(bytes);
        } else if (messageType == ActiveMQMapMessage.DATA_STRUCTURE_TYPE) {
           return asMap(msg).getString("test1");
        } else if (messageType == ActiveMQObjectMessage.DATA_STRUCTURE_TYPE) {
            return asObject(msg).getObject().toString();
        } else {
            throw new IllegalArgumentException("Unsupported data structure type: " + messageType);
        }
    }

    private Object rawBody(final ActiveMQMessage msg) {
        if (messageType == ActiveMQTextMessage.DATA_STRUCTURE_TYPE) {
            return asText(msg).text;
        } else if (messageType == ActiveMQBytesMessage.DATA_STRUCTURE_TYPE) {
            return asBytes(msg).content != null ? new String(asBytes(msg).content.data) : null;
        } else if (messageType == ActiveMQStreamMessage.DATA_STRUCTURE_TYPE) {
            return asStream(msg).content != null ? new String(asStream(msg).content.data) : null;
        } else if (messageType == ActiveMQMapMessage.DATA_STRUCTURE_TYPE) {
            Map<String, Object> map = asMap(msg).map;
            return map != null ? map.get("test1") : null;
        } else if (messageType == ActiveMQObjectMessage.DATA_STRUCTURE_TYPE) {
            return asObject(msg).object;
        } else {
            throw new IllegalArgumentException("Unsupported data structure type: " + messageType);
        }
    }

    private static ActiveMQTextMessage asText(ActiveMQMessage message) {
        return (ActiveMQTextMessage) message;
    }

    private static ActiveMQBytesMessage asBytes(ActiveMQMessage message) {
        return (ActiveMQBytesMessage) message;
    }

    private static ActiveMQStreamMessage asStream(ActiveMQMessage message) {
        return (ActiveMQStreamMessage) message;
    }

    private static ActiveMQMapMessage asMap(ActiveMQMessage message) {
        return (ActiveMQMapMessage) message;
    }

    private static ActiveMQObjectMessage asObject(ActiveMQMessage message) {
        return (ActiveMQObjectMessage) message;
    }
}
