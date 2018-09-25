package io.cantor.service.clients;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.typesafe.config.Config;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.cantor.service.clients.storage.Storage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimeWatcher {

    private static final int DEFAULT_MAX_DELAYED_SECONDS = 600;
    private static final String DELAY = "time.watcher.delay";
    private static final int DEFAULT_DELAY = 1;
    private static final long BEGINNING = 0;

    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean stopped = new AtomicBoolean(false);

    @Getter
    private String localId;

    private AtomicLong availableTimestamp;
    private ScheduledExecutorService watchExecutor;
    private int watchDelay;
    private List<Storage> storages;
    private int maxDelayed;
    private Map<String, Integer> instancesBox;

    public TimeWatcher(Config config, List<Storage> storages, String localId,
                       Map<String, Integer> instancesBox) {

        this.localId = localId;
        this.storages = storages;
        this.maxDelayed = config.hasPath("storage.max.delay.seconds") ? config.getInt(
                "storage.max.delay.seconds") : DEFAULT_MAX_DELAYED_SECONDS;
        availableTimestamp = new AtomicLong(0);
        this.instancesBox = instancesBox;

        Thread.UncaughtExceptionHandler handler = (t, e) -> {
            if (log.isErrorEnabled())
                log.error("time watcher thread error [thread {}]", t.getId(), e);
        };

        ThreadFactoryBuilder builder = (new ThreadFactoryBuilder()).setDaemon(false)
                                                                   .setNameFormat("time-watcher-%s")
                                                                   .setUncaughtExceptionHandler(
                                                                           handler);
        watchDelay = config.hasPath(DELAY) ? config.getInt(DELAY) : DEFAULT_DELAY;
        watchExecutor = Executors.newSingleThreadScheduledExecutor(builder.build());
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            watchExecutor.scheduleAtFixedRate(() -> {
                try {
                    watch();
                } catch (Exception e) {
                    if (log.isErrorEnabled())
                        log.error("", e);
                }
            }, 0, watchDelay, TimeUnit.SECONDS);
        } else {
            if (log.isDebugEnabled())
                log.debug("time watcher is already started");
        }
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true))
            return;
        if (null != watchExecutor) {
            watchExecutor.shutdown();

            try {
                TimeUnit.SECONDS.sleep(30);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            if (!watchExecutor.isShutdown())
                watchExecutor.shutdownNow();
        }
    }

    /**
     * Return the availableTimestamp updated according to the time lattice in the storage if the
     * {@link TimeWatcher} works well. Or it returns the local timestamp if the {@link TimeWatcher}
     * has failed to update the availableTimestamp for a period of time, which is larger than
     * MAX_DELAYED_SECONDS.
     *
     * @return the available timestamp
     */
    public long currentAvailableTimestamp() {
        long local = localTimestamp();
        long fromLattice = availableTimestamp.get();
        return maxDelayed <= (local - fromLattice) ? local : fromLattice;
    }

    public long instanceId() {
        Storage first = storages.get(0);
        return first.descriptor() + (long) instancesBox.get(first.type());
    }

    private long localTimestamp() {
        return System.currentTimeMillis() / 1000;
    }

    private void watch() {
        log.info("watch and sync the timestamp");
        // update local time
        long availableLocalTs = BEGINNING;
        long localCurrentTime = localTimestamp();
        for (Storage storage : storages) {
            long tmpTs = storage.syncTime(localCurrentTime);
            if (BEGINNING == availableLocalTs) {
                availableLocalTs = tmpTs;
            }
        }
        availableLocalTs = BEGINNING == availableLocalTs ? localCurrentTime : availableLocalTs;

        // get the max time and reset the available time as it if the max time >= availableLocalTs
        long maxTime = 0;
        try {
            maxTime = getMaxTime();
        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error("Time watcher get max time from lattice failed", e);
        }

        long diff = maxTime - availableLocalTs;
        if (diff > maxDelayed) {
            if (log.isErrorEnabled())
                log.error("Time watcher {} has expired too much: {}", localId, diff);
        }

        if (log.isDebugEnabled())
            log.debug("new max time is {}", maxTime);
        availableTimestamp.set(maxTime >= availableLocalTs ? maxTime : availableLocalTs);

        heartbeat();
    }

    private void heartbeat() {
        storages.forEach(
                s -> s.heartbeat(instancesBox.get(s.type()), Storage.DEFAULT_HEARTBEAT_SECONDS));
    }

    private long getMaxTime() throws Exception {
        if (log.isDebugEnabled())
            log.debug("Get max time from Storage");

        long maxTime = 0;
        for (Storage storage : storages) {
            if (!storage.available()) {
                if (log.isWarnEnabled())
                    log.warn(String.format("Storage %s is not available", storage.type()));

                continue;
            }

            List<Long> timeMeta = storage.timeMeta();
            Optional<Long> opt = timeMeta.stream().max((t1, t2) -> ((Long) (t1 - t2)).intValue());
            if (opt.isPresent()) {
                maxTime = opt.get();
                break;
            }
        }

        return maxTime;
    }
}
