package io.cantor.sdk;


import org.jctools.queues.SpmcArrayQueue;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class SequenceProducer {
    public final long EPOCH = 1514736000L;

    public static final int FEEDING_IDLE = 60;
    public static final long EXPIRED_GAP_IN_SECONDS =  10 * 60;
    private ServiceCaller sequenceClient;
    private final long range;
    private ConcurrentHashMap<Long, QueueHolder> queueHolders = new ConcurrentHashMap<>();
    private ExecutorService executor = Executors.newFixedThreadPool(1, r -> {
        Thread t = new Thread(r, "producer thread");
        t.setDaemon(true);
        return t;
    });
    private Lock feedingLock = new ReentrantLock();
    private Condition feedingCdt = feedingLock.newCondition();

    public SequenceProducer(String host, int port, long range) {
        this(new ServiceCaller(host, port), range);
    }

    SequenceProducer(ServiceCaller sequenceClient, long range) {
        this.sequenceClient = sequenceClient;
        this.range = range;
        feed();
    }

    private void feed() {
        executor.execute(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                boolean waiting = true;
                Collection<QueueHolder> qhs = queueHolders.values();
                int snapshotSize = qhs.size();
                for (QueueHolder qh : qhs) {
                    boolean touched = qh.touch(null);
                    if (touched) {
                        qh.lock.lock();
                        try {
                            int queueSize = qh.queue.size();
                            if (queueSize != 0) {
                                qh.seekingCdt.signal();
                            }
                        } finally {
                            qh.lock.unlock();
                        }
                    }
                    waiting = waiting && !touched;
                }

                if (waiting) {
                    feedingLock.lock();
                    try {
                        int newSize = queueHolders.size();
                        if (newSize != snapshotSize) {
                            if (log.isWarnEnabled())
                                log.warn("new holders found [{}, {}]", snapshotSize, newSize);
                            continue;
                        }
                        try {
                            feedingCdt.await(FEEDING_IDLE, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            if (log.isWarnEnabled())
                                log.warn("interrupted", e);
                        }
                    } finally {
                        feedingLock.unlock();
                    }
                }
            }
        });
    }

    public Sequence produce(long category) {
        AtomicBoolean createNew = new AtomicBoolean();
        QueueHolder queueHolder = queueHolders.computeIfAbsent(category, k -> {
            createNew.set(true);
            return new QueueHolder(k);
        });
        if (createNew.get()) {
            signalFeeding();
        }
        return queueHolder.poll();
    }

    private class QueueHolder {
        @Getter
        private SpmcArrayQueue<Sequence> queue = new SpmcArrayQueue<>(1024);
        private volatile Range pendingSequence;
        private final long category;
        private Lock lock = new ReentrantLock();
        private Condition seekingCdt = lock.newCondition();

        private QueueHolder(long category) {
            this.category = category;
        }

        public boolean touch(Long timeout) {
            if (pendingSequence == null || pendingSequence.peek() == null) {
                RequestBuilder.SequenceRequest request = sequenceClient.newRequestBuilder()
                                                                       .category(category)
                                                                       .range(SequenceProducer.this.range)
                                                                       .timeout(timeout)
                                                                       .build();
                try {
                    pendingSequence = sequenceClient.call(request);
                } catch (Exception e) {
                    return false;
                }
            }
            Sequence newId = pendingSequence.peek();
            if (queue.offer(newId)) {
                pendingSequence.getAndIncrement();
                return true;
            } else {
                return false;
            }
        }

        public Sequence poll() {
            Sequence newId = null;
            while (newId == null) {
                newId = queue.poll();
                if (newId == null) {
                    lock.lock();
                    try {
                        signalFeeding();
                        boolean onTime = seekingCdt.await(5, TimeUnit.SECONDS);
                        newId = queue.poll();
                        if (!onTime && newId == null) {
                            if (log.isWarnEnabled())
                                log.warn("no id got after waiting, on time {}", onTime);
                            break;
                        }
                    } catch (InterruptedException e) {
                        if (log.isWarnEnabled())
                            log.warn("interrupted", e);
                    } finally {
                        lock.unlock();
                    }
                }
                if (newId != null &&
                        (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - EPOCH - newId.timestamp()) >= EXPIRED_GAP_IN_SECONDS) {
                    newId = null;
                }
            }
            return newId;
        }
    }

    private void signalFeeding() {
        this.feedingLock.lock();
        try {
            this.feedingCdt.signal();
        } finally {
            this.feedingLock.unlock();
        }
    }
}
