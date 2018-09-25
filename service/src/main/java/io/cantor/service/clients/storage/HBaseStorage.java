package io.cantor.service.clients.storage;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.typesafe.config.Config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class HBaseStorage implements Storage {

    private static final long CATE_HBASE = 0L;

    private static final long REGISTERED = 1L;
    private static final int CHECK_ACTIVE_DELAY = 1;
    private static final long BEGINNING = 0;
    private static final String TYPE = "HBase";

    private static final byte[] SERVICE_FAMILY = Bytes.toBytes("svc");
    private static final byte[] INST_FAMILY = Bytes.toBytes("inst");
    private static final String NAMESPACE = "infra_pub";
    private static final String TABLE_FMT = "id-gen-%s";
    private static final String META_TABLE = "id-gen-meta";
    private static final String ROW_KEY_FMT = "%s";
    private static final int TABLE_COUNT = 10;
    private static final int TABLE_OPERATION_TIMEOUT = 500; // ms
    private static final long DEFAULT_TTL = 86400L * 1000L; // ms
    private final byte[] HBASE_LATTICE_KEY = Bytes.toBytes("time_lattice");
    private static final String RUNNING_STATE_FMT = "running_state_%s";

    private final int tableCount;
    private final long ttl;
    private Connection connection;
    private Table metaTable;
    private Configuration hbaseConf;
    private ConcurrentHashMap<String, Table> tableConnections = new ConcurrentHashMap<>();
    private ScheduledExecutorService executorService;
    private volatile boolean available = false;
    private String localId;
    private final byte[] hbaseTimeLatticeCol;

    HBaseStorage(Config config, String localId) throws Exception {
        this.localId = localId;
        this.hbaseTimeLatticeCol = Bytes.toBytes(localId);
        tableCount = config.hasPath("hbase.table.count") ? config.getInt(
                "hbase.table.count") : TABLE_COUNT;
        ttl = config.hasPath("hbase.ttl") ? config.getLong("hbase.ttl") * 1000L : DEFAULT_TTL;

        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.client.retries.number", "1");
        hbaseConf.set("hbase.zookeeper.quorum", config.getString("zookeeper.quorum"));
        hbaseConf.set("hbase.zookeeper.property.clientPort", config.getString("zookeeper.port"));
        hbaseConf.set("zookeeper.znode.parent", config.getString("zookeeper.znode.parent"));
        hbaseConf.set("hbase.hconnection.threads.max",
                config.getString("hbase.hconnection.threads.max"));
        hbaseConf.set("hbase.hconnection.threads.core",
                config.getString("hbase.hconnection.threads.core"));

        connection = ConnectionFactory.createConnection(hbaseConf);
        createTableConnections(tableCount);
        metaTable = getTable(NAMESPACE, META_TABLE);

        ThreadFactory factory = (new ThreadFactoryBuilder()).setDaemon(false)
                                                            .setNameFormat("hbase-probe-%s")
                                                            .setUncaughtExceptionHandler((t, e) -> {
                                                                if (log.isErrorEnabled())
                                                                    log.error(
                                                                            "hbase heartbeat thread error [thread {}]",
                                                                            t.getId(), e);
                                                            })
                                                            .build();
        executorService = Executors.newSingleThreadScheduledExecutor(factory);
        checkConn();
        // tricky: check hbase again, interrupts the creation process by exceptions if it fails
        HBaseAdmin.checkHBaseAvailable(hbaseConf);
    }

    /**
     * @return the value before increment
     */
    @Override
    public Optional<Long> incrementAndGet(long category, long ts, long range) {
        String tbl = String.format(TABLE_FMT, category % TABLE_COUNT);
        Table table = tableConnections.get(tbl);

        try {
            Increment increment = new Increment(Bytes.toBytes(String.format(ROW_KEY_FMT, ts)));
            increment.setTTL(ttl);
            byte[] col = Bytes.toBytes(String.valueOf(category));
            increment.addColumn(SERVICE_FAMILY, col, range);
            Result result = table.increment(increment);
            Long afterInc = Bytes.toLong(result.getValue(SERVICE_FAMILY, col));

            return Optional.of(afterInc);
        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error(
                        "increment range value failed for [ category: {} ] [ timestamp {} ] [ range {} ]",
                        category, ts, range, e);
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        if (null != executorService) {
            executorService.shutdownNow();
        }
        tableConnections.forEach((tname, tbl) -> {
            try {
                tbl.close();
            } catch (IOException e) {
                if (log.isWarnEnabled())
                    log.warn("close table {} failed. skipped", new String(tname.getBytes()), e);
            }
        });
        if (null != connection)
            try {
                connection.close();
            } catch (IOException e) {
                if (log.isErrorEnabled())
                    log.error("Close hbase connection failed", e);
            }
    }

    @Override
    public boolean available() {
        return available && !connection.isAborted() && !connection.isClosed();
    }

    @Override
    public long syncTime(long localTime) {
        Get get = (new Get(HBASE_LATTICE_KEY)).addColumn(INST_FAMILY, hbaseTimeLatticeCol);

        try {
            Result result = metaTable.get(get);
            byte[] value = result.getValue(INST_FAMILY, hbaseTimeLatticeCol);

            long timeSnapshot =
                    null != value && value.length > 0 ? Bytes.toLong(value, 0) : BEGINNING;
            if (log.isDebugEnabled())
                log.debug("[HBase] time snapshot is {} and local current is {}", timeSnapshot,
                        localTime);
            if (timeSnapshot < localTime) {
                Put put = (new Put(HBASE_LATTICE_KEY)).addColumn(INST_FAMILY, hbaseTimeLatticeCol,
                        Bytes.toBytes(localTime));
                metaTable.put(put);
            }
        } catch (IOException e) {
            if (log.isErrorEnabled())
                log.warn("[HBase] update timestamp snapshot failed for {}. local timestamp is {}",
                        localId, localTime, e);
        }

        return localTime;

    }

    @Override
    public List<Long> timeMeta() {
        List<Long> times = new ArrayList<>();

        Get get = (new Get(HBASE_LATTICE_KEY)).addFamily(INST_FAMILY);
        Result result;
        try {
            result = metaTable.get(get);
            List<Cell> cells = result.listCells();
            if (log.isDebugEnabled())
                log.debug("Time lattice is {}", cells.stream()
                                                     .map(c -> Bytes.toLong(c.getValueArray(),
                                                             c.getValueOffset()))
                                                     .collect(Collectors.toList()));
            for (Cell cell : cells) {
                long current = Bytes.toLong(cell.getValueArray(), cell.getValueOffset());
                times.add(current);
            }
        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error("get time lattice from hbase failed", e);
        }

        return times;
    }

    @Override
    public void deregister() {
        if (null != metaTable) {
            try {
                Delete delete = new Delete(HBASE_LATTICE_KEY);
                delete.addColumn(INST_FAMILY, Bytes.toBytes(localId));
                metaTable.delete(delete);
                metaTable.close();
            } catch (Exception e) {
                if (log.isWarnEnabled())
                    log.warn("close Hbase table failed {}:{}", NAMESPACE, META_TABLE, e);
            }
        }
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public long descriptor() {
        return CATE_HBASE;
    }

    @Override
    public int checkAndRegister(int maxInstances) {
        int i = 0;
        int instanceNumber = ILLEGAL_INSTANCE;
        while (i < maxInstances) {
            try {
                Increment increment = new Increment(
                        Bytes.toBytes(String.format(RUNNING_STATE_FMT, i)));
                byte[] col = Bytes.toBytes("state");
                increment.addColumn(INST_FAMILY, col, 1);
                Result result = metaTable.increment(increment);
                Long afterInc = Bytes.toLong(result.getValue(INST_FAMILY, col));
                if (afterInc == REGISTERED) {
                    instanceNumber = i;
                    heartbeat(instanceNumber, DEFAULT_HEARTBEAT_SECONDS * 1000);
                    break;
                } else {
                    if (log.isWarnEnabled())
                        log.warn("[HBase] Failed to register since the instance box {} is full.",
                                i);
                }
            } catch (Exception e) {
                if (log.isErrorEnabled())
                    log.error(String.format("[HBase] Failed to check and register on %s.", i), e);
            }

            i++;
        }

        return instanceNumber;
    }

    @Override
    public boolean heartbeat(int instanceNumber, int ttl) {
        try {
            Increment increment = new Increment(
                    Bytes.toBytes(String.format(RUNNING_STATE_FMT, instanceNumber)));
            byte[] col = Bytes.toBytes("state");
            increment.addColumn(INST_FAMILY, col, 1);
            increment.setTTL((long) ttl);
            metaTable.increment(increment);

            return true;
        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error("[HBase] Failed to heartbeat.", e);

            return false;
        }
    }

    private Table getTable(String namespace, String tableName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        table.setOperationTimeout(TABLE_OPERATION_TIMEOUT);
        return table;
    }

    private void checkConn() {
        try {
            HBaseAdmin.checkHBaseAvailable(hbaseConf);
        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error("HBase is not available", e);
            available = false;
        }
        if (!available || null == connection || connection.isAborted() || connection.isClosed()) {
            available = false;
            synchronized (this) {
                try {
                    connection = ConnectionFactory.createConnection(hbaseConf);
                    HBaseAdmin.checkHBaseAvailable(hbaseConf);
                    createTableConnections(tableCount);
                    metaTable = getTable(NAMESPACE, META_TABLE);
                    available = true;
                } catch (Exception e) {
                    if (log.isErrorEnabled())
                        log.error("[HBase] Connect to HBase failed");
                }
            }
        }

        executorService.schedule(this::checkConn, CHECK_ACTIVE_DELAY, TimeUnit.SECONDS);
    }

    private void createTableConnections(int tableCount) throws Exception {
        for (int i = 0; i < tableCount; i++) {
            String tbl = String.format(TABLE_FMT, i);
            // default queue for batch in HTable is the LinkedBlockingQueue
            Table table = getTable(NAMESPACE, tbl);
            tableConnections.put(tbl, table);
        }
    }

}
