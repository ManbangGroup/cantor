package io.cantor.service.rest;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.math3.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.cantor.http.AffinityScheduler;
import io.cantor.http.AppRequestResponseHandler;
import io.cantor.http.HandlerRequest;
import io.cantor.http.HandlerResponse;
import io.cantor.service.Utils;
import io.cantor.service.clients.LocalIdGenerator;
import io.cantor.service.clients.Parser;
import io.cantor.service.clients.TimeWatcher;
import io.cantor.service.clients.storage.Storage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IdGenerator implements
        AppRequestResponseHandler<AffinityScheduler, HandlerRequest, HandlerResponse> {


    private static final ExecutionException LOAD_CACHE_EXCEPTION = new ExecutionException(
            "load cache failed", new Throwable("load cache failed"));
    private static final ExecutionException LOAD_CACHE_CLIENT_EXCEPTION = new ExecutionException(
            "load cache failed, client is unavailable",
            new Throwable("load cache failed, client is unavailable"));

    private static final String CATEGORY = "cate";
    private static final String RANGE = "range";
    private static final String MODE = "mode";
    private static final String START = "start";

    private static final long DEFAULT_RANGE = 1000L;
    private static final long MAX_RANGE = 10000L;
    private static final long DEFAULT_CATEGORY = 0L;
    private static final long MAX_CATEGORY = 255L;
    private static final long CACHE_RANGE = MAX_RANGE * 10;

    private static final long ILLEGAL_SEQ = -1L;
    private static final long START_SEQ = 1L;

    private static final int SEQ_IDX = 0;
    private static final int DESC_IDX = 1;
    private static final int TS_IDX = 2;
    private static final int RANGE_IDX = 3;
    private static final long EXPIRE_DURATION = 600L;

    private final List<Storage> storages;
    private final TimeWatcher timeWatcher;
    private LocalIdGenerator localIdGenerator;
    private Cache<String, CacheSequence> sequenceCache;
    private Cache<String, AtomicBoolean> sequenceLocks;

    public IdGenerator(List<Storage> storages, TimeWatcher timeWatcher) {
        localIdGenerator = new LocalIdGenerator();
        this.timeWatcher = timeWatcher;
        this.storages = storages;

        sequenceLocks = CacheBuilder.newBuilder().build();
        sequenceCache = CacheBuilder.<String, CacheSequence>newBuilder().expireAfterWrite(
                EXPIRE_DURATION, TimeUnit.SECONDS).removalListener(notification -> {
            String key = (String) notification.getKey();
            if (null != key)
                sequenceLocks.invalidate(key);
        }).build();
    }

    @Override
    public void handle(AffinityScheduler scheduler, HandlerRequest req, HandlerResponse resp) {
        resp.header("server", timeWatcher.localId());
        resp.header("Content-Type", "application/json");
        Map<String, String> queries = req.queries();
        long category;
        if (!queries.containsKey(CATEGORY) || queries.get(CATEGORY).isEmpty())
            category = DEFAULT_CATEGORY;
        else
            category = Long.valueOf(String.valueOf(queries.get(CATEGORY)));
        if (category > MAX_CATEGORY) {
            resp.badRequest(
                    String.format("Illegal category: %s, max value is 255", category).getBytes());
            return;
        }

        long range;
        if (!queries.containsKey(RANGE) || queries.get(RANGE).isEmpty()) {
            range = DEFAULT_RANGE;
        } else {
            range = Long.valueOf(String.valueOf(queries.get(RANGE)));
            range = range <= 0 ? DEFAULT_RANGE : range;
        }
        range = MAX_RANGE < range ? MAX_RANGE : range;

        int mode;
        if (!queries.containsKey(MODE) || queries.get(MODE).isEmpty())
            mode = Parser.WHOLE_ID;
        else
            mode = Integer.valueOf(queries.get(MODE));
        if (mode == Parser.RADIX)
            range = range >= 10 ? 10 : range;

        // generate ids
        long ts = timeWatcher.currentAvailableTimestamp() - Parser.START_EPOCH;
        long instanceId = timeWatcher.instanceId();
        Optional<Long[]> opt = getSequence(category, ts, range, instanceId);
        if (!opt.isPresent()) {
            resp.internalServerError("Can not get an id".getBytes());
            return;
        }
        Long[] val = opt.get();
        long seq = val[SEQ_IDX];
        if (!Parser.validSequence(seq)) {
            resp.forbidden("Sequence at current timestamp is full".getBytes());
            return;
        }
        long desc = val[DESC_IDX];
        if (LocalIdGenerator.LOCAL_CATE == desc)
            ts = val[TS_IDX];
        range = val[RANGE_IDX];

        // combine values
        Map<String, Object> respResult;
        Parser.Serializer serializer;
        switch (mode) {
            case Parser.WHOLE_ID:
                serializer = Parser.serialize(category, desc, ts, seq, instanceId);
                respResult = ImmutableMap.of(START, String.valueOf(serializer.id()), RANGE, range);
                break;
            case Parser.RADIX:
                serializer = Parser.serialize(category, desc, ts, seq, instanceId);
                respResult = ImmutableMap.of(START, serializer.toString(Parser.RADIX_36), RANGE,
                        range);
                break;
            default:
                resp.badRequest(String.format("Illegal mode: %s", mode).getBytes());
                return;
        }

        resp.ok(Utils.convertToString(respResult).getBytes());
    }

    private Optional<Long[]> getSequence(long category, long ts, long range, long instanceId) {
        long seq = ILLEGAL_SEQ;
        long descriptor = 0L;
        long actualTs = ts;
        Optional<Long> opt;
        Storage storage = null;
        for (Storage currentStorage : storages) {
            if (!currentStorage.available())
                continue;
            storage = currentStorage;
            descriptor = storage.descriptor();
            break;
        }

        if (null != storage) {
            opt = getFromCache(category, actualTs, range, storage, instanceId);
            if (opt.isPresent())
                seq = opt.get();
        }

        if (ILLEGAL_SEQ == seq) {
            // from local
            descriptor = LocalIdGenerator.LOCAL_CATE;
            Pair<Long, Long> seqParts = localIdGenerator.getFromLocal(category, range, instanceId);
            actualTs = seqParts.getFirst();
            seq = seqParts.getSecond();
        }

        return Optional.of(new Long[]{seq, descriptor, actualTs, range});
    }

    private Optional<Long> getFromCache(long category, long ts, long range, Storage storage, long instanceId) {
        Long seq;
        String key = String.format("%s-%s-%s", ts, category, instanceId);
        CacheSequence cacheSequence = sequenceCache.getIfPresent(key);
        if (null != cacheSequence) {
            seq = cacheSequence.next();
            if (null == seq)
                sequenceCache.invalidate(key);
            return Optional.ofNullable(seq);
        }

        AtomicBoolean lock = null;
        try {
            lock = sequenceLocks.get(key, () -> new AtomicBoolean(true));
        } catch (ExecutionException e) {
            if (log.isWarnEnabled())
                log.warn("get lock cache failed for [{}]", key, e);
        }
        if (null == lock || !lock.compareAndSet(true, false))
            return Optional.empty();
        try {
            cacheSequence = sequenceCache.get(key, () -> {
                if (!storage.available())
                    throw LOAD_CACHE_CLIENT_EXCEPTION;
                Optional<Long> opt = storage.incrementAndGet(category, ts, CACHE_RANGE);
                if (!opt.isPresent()) {
                    if (log.isErrorEnabled())
                        log.error(
                                "get and increment in failed for [cate {}] [ts {}] [range {}] in {}",
                                category, ts, range, storage.getClass().getSimpleName());
                    throw LOAD_CACHE_EXCEPTION;
                }
                long incrementedSeq = opt.get();
                return new CacheSequence(incrementedSeq - CACHE_RANGE + START_SEQ, incrementedSeq,
                        range);
            });
        } catch (ExecutionException e) {
            if (log.isWarnEnabled())
                log.warn("", e);
        }
        lock.compareAndSet(false, true);

        if (null == cacheSequence)
            return Optional.empty();
        seq = cacheSequence.next();
        if (null == seq)
            sequenceCache.invalidate(key);

        return Optional.ofNullable(seq);
    }

    static class CacheSequence {

        @Getter
        private AtomicLong seq;

        private final long seqLimit;
        private final long step;

        CacheSequence(long seq, long seqLimit, long step) {
            this.seq = new AtomicLong(seq);
            this.seqLimit = seqLimit;
            this.step = step;
        }

        Long next() {
            Long next = seq.getAndAdd(step);
            return next < seqLimit ? next : null;
        }
    }
}
