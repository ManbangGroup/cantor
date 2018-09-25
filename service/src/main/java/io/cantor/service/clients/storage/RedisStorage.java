package io.cantor.service.clients.storage;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.typesafe.config.Config;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

@Slf4j
class RedisStorage implements Storage {

    private static final long CATE_REDIS = 1L;
    private static final long REGISTERED = 1L;

    private static final int CHECK_ACTIVE_DELAY = 1;
    private static final String TYPE = "Redis";
    private static final String PONG_RESPONSE = "PONG";

    private static final String TIMESTAMP_KEY_FMT = "%s";
    private static final String RUNNING_STATE_FMT = "running_state_%s";
    private static final int DEFAULT_TTL = 86400;
    private static final String NULL = "nil";
    private static final long BEGINNING = 0;
    private static final String TIMESTAMP_KEY = "time_lattice";
    private static final String INSTANCES_INDEX = "instances_index";

    private final int ttl;
    private final String host;
    private final int port;
    private volatile Jedis jedis;
    private ScheduledExecutorService executorService;
    private volatile boolean active = false;
    private String localId;
    private AtomicBoolean ok = new AtomicBoolean(true);

    RedisStorage(Config config, String localId) throws Exception {
        this.localId = localId;
        this.host = config.getString("redis.host");
        this.port = config.getInt("redis.port");

        this.ttl = config.hasPath("redis.ttl") ? config.getInt("redis.ttl") : DEFAULT_TTL;
        ThreadFactory factory = (new ThreadFactoryBuilder()).setDaemon(false)
                                                            .setNameFormat("redis-probe-%s")
                                                            .setUncaughtExceptionHandler((t, e) -> {
                                                                if (log.isErrorEnabled())
                                                                    log.error(
                                                                            "redis heartbeat thread error [thread {}]",
                                                                            t.getId(), e);
                                                            })
                                                            .build();
        executorService = Executors.newSingleThreadScheduledExecutor(factory);
        active = connect();
        if (!active) {
            throw new ConnectException("can not connect to Redis");
        }
        executorService.scheduleAtFixedRate(this::checkConn, CHECK_ACTIVE_DELAY, CHECK_ACTIVE_DELAY,
                TimeUnit.SECONDS);
    }

    @Override
    public Optional<Long> incrementAndGet(long category, long ts, long range) {
        if (!available())
            return Optional.empty();

        String timestampKey = String.format(TIMESTAMP_KEY_FMT, ts);

        Long after = null;
        checkBusyAndBlock();
        try {
            after = jedis.hincrBy(timestampKey, String.format("cate-%s", category), range);
            if (range == after)
                jedis.expire(timestampKey, ttl);

        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error("connect to redis failed", e);
        }

        release();
        return Optional.ofNullable(after);
    }

    @Override
    public void close() {
        if (null != executorService) {
            executorService.shutdownNow();
        }
        if (null != jedis) {
            jedis.close();
        }
    }

    @Override
    public boolean available() {
        return active && null != jedis && jedis.isConnected();
    }

    @Override
    public long syncTime(long localCurrentTime) {
        if (log.isDebugEnabled())
            log.debug("Sync time from Redis");

        long time = BEGINNING;
        try {
            Optional<String> opt = getField(TIMESTAMP_KEY, localId);
            if (!opt.isPresent()) {
                if (log.isWarnEnabled())
                    log.warn("[Redis] get time snapshot failed: {}", localId);
            }
            long timeSnapshot = opt.map(Long::valueOf).orElse(BEGINNING);
            if (log.isDebugEnabled())
                log.debug("[Redis] time snapshot is {} and local current is {}", timeSnapshot,
                        localCurrentTime);
            if (timeSnapshot < localCurrentTime)
                setField(TIMESTAMP_KEY, localId, String.valueOf(localCurrentTime));

            time = timeSnapshot < localCurrentTime ? localCurrentTime : timeSnapshot;
        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error("Time watcher update redis time lattice failed", e);
        }

        log.info("Done Sync from Redis");
        return time;
    }

    @Override
    public List<Long> timeMeta() {
        List<Long> times = new ArrayList<>();

        checkBusyAndBlock();
        try {
            Map<String, String> result = jedis.hgetAll(TIMESTAMP_KEY);
            if (null != result && !result.isEmpty())
                result.forEach((id, ts) -> times.add(Long.valueOf(ts)));

        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error("[Redis] Failed to get time meta from redis", e);
        }

        log.info("Done in TimeMeta Redis");
        release();
        return times;
    }

    @Override
    public void deregister() {
        try {
            deleteField(TIMESTAMP_KEY, localId);
        } catch (Exception e) {
            if (log.isWarnEnabled())
                log.warn("delete redis time lattice failed", e);
        }
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public long descriptor() {
        return CATE_REDIS;
    }

    @Override
    public int checkAndRegister(int maxInstances) {
        int i = 0;
        int instanceNumber = ILLEGAL_INSTANCE;
        while (i < maxInstances) {
            try {
                String key = String.format(RUNNING_STATE_FMT, i);
                Long state = jedis.incr(key);
                if (null != state && state == REGISTERED) {
                    instanceNumber = i;
                    heartbeat(instanceNumber, Storage.DEFAULT_HEARTBEAT_SECONDS);
                    break;
                } else {
                    if (log.isErrorEnabled())
                        log.error(String.format("[Redis] Failed to check instances state of %s", key));
                }
            } catch (Exception e) {
                if (log.isErrorEnabled())
                    log.error("[Redis] Failed to check and register", e);
            }

            i++;
        }

        return instanceNumber;
    }

    @Override
    public boolean heartbeat(int instanceNumber, int ttl) {
        try {
            checkBusyAndBlock();
            String key = String.format(RUNNING_STATE_FMT, instanceNumber);
            Long after = jedis.expire(key, ttl);
            if (null == after) {
                if (log.isWarnEnabled())
                    log.warn("[Redis] Failed to update the heartbeat expired time for {}",
                            instanceNumber);

                release();
                return false;
            }

            release();
            return true;
        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error("[Redis] Failed to heartbeat");

            release();
            return false;
        }
    }

    private void checkBusyAndBlock() {
        while (!ok.compareAndSet(true, false)) {
            try {
                TimeUnit.NANOSECONDS.sleep(100);
            } catch (InterruptedException e) {
                if (log.isErrorEnabled())
                    log.error("", e);

                release();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void release() {
        ok.compareAndSet(false, true);
    }

    private Optional<String> getField(String key, String field) {
        String result = null;
        checkBusyAndBlock();
        try {
            result = jedis.hget(key, field);
            if (null == result || NULL.equals(result))
                result = null;

        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error(String.format("[Redis] Failed to get key: %s, field: %s", key, field), e);
        }

        release();
        return Optional.ofNullable(result);
    }

    private void setField(String key, String field, String value) {
        checkBusyAndBlock();
        try {
            jedis.hset(key, field, value);
        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error(String.format("[Redis] HSet failed, key: %s, field :%s, value: %s", key,
                        field, value));
        }

        release();
    }

    private void deleteField(String key, String field) {

        checkBusyAndBlock();
        try {
            jedis.hdel(key, field);
        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error(String.format("[Redis] Failed to HDel, key: %s, field: %s", key, field),
                        e);
        }

        release();
    }

    private void checkConn() {
        log.info("[Redis] Check redis connections");
        if (null == jedis) {
            active = false;
            active = connect();
        }

        checkBusyAndBlock();
        String resp = jedis.ping();
        if (!PONG_RESPONSE.equals(resp)) {
            active = false;
            active = connect();
        }

        release();
    }

    private synchronized boolean connect() {
        boolean success = false;
        if (null != this.jedis) {
            try {
                log.info("reconnect to Redis");
                this.jedis.close();
            } catch (Exception e) {
                if (log.isErrorEnabled())
                    log.error("Can not connect to Redis", e);
            }
        }

        try {
            this.jedis = new Jedis(host, port);
            success = true;
        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error("Can not connect to Redis", e);
        }

        return success;
    }
}