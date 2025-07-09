package com.interview.timeseries;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.io.File;
import java.io.IOException;
import java.util.NavigableMap;

/**
 * Implementation of the TimeSeriesStore interface.
 */
public class TimeSeriesStoreImpl implements TimeSeriesStore {

    // In-memory store: metric -> (timestamp -> list of data points)
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, List<DataPoint>>> store = new ConcurrentHashMap<>();

    // Read-write lock for thread safety (coarse-grained)
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // Persistence manager for write-ahead log
    private PersistenceManager persistenceManager;
    private final long retentionMillis = TimeUnit.HOURS.toMillis(24);
    private final ScheduledExecutorService retentionExecutor = Executors.newSingleThreadScheduledExecutor();


    @Override
    public boolean insert(long timestamp, String metric, double value, Map<String, String> tags) {
        long cutoff = System.currentTimeMillis() - retentionMillis;
        if (timestamp < cutoff) return false;

        rwLock.writeLock().lock();
        try {
            DataPoint dp = new DataPoint(timestamp, metric, value, tags);

            store
                    .computeIfAbsent(metric, m -> new ConcurrentSkipListMap<>())
                    .computeIfAbsent(timestamp, t -> new ArrayList<>())
                    .add(dp);

            try {
                persistenceManager.append(dp);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }

            return true;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public List<DataPoint> query(String metric, long timeStart, long timeEnd, Map<String, String> filters) {
        rwLock.readLock().lock();
        try {
            ConcurrentSkipListMap<Long, List<DataPoint>> metricMap = store.get(metric);
            if (metricMap == null) return new ArrayList<>();

            NavigableMap<Long, List<DataPoint>> sub = metricMap.subMap(timeStart, true, timeEnd, false);
            List<DataPoint> result = new ArrayList<>();

            for (List<DataPoint> dps : sub.values()) {
                for (DataPoint dp : dps) {
                    if (matchesFilters(dp, filters)) {
                        result.add(dp);
                    }
                }
            }

            return result;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private boolean matchesFilters(DataPoint dp, Map<String, String> filters) {
        if (filters == null || filters.isEmpty()) return true;

        for (Map.Entry<String, String> filter : filters.entrySet()) {
            String actualValue = dp.getTags().get(filter.getKey());
            if (actualValue == null || !actualValue.equals(filter.getValue())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean initialize() {
        try {
            retentionExecutor.scheduleAtFixedRate(this::performRetentionCleanup, 60, 60, TimeUnit.SECONDS);
            // Use working directory for WAL
            String path = System.getProperty("user.dir") + File.separator + "timeseries.log";
            persistenceManager = new PersistenceManager(path);
            long cutoff = System.currentTimeMillis() - retentionMillis;
            persistenceManager.replayStreaming(this::insertWithoutLog, cutoff);

            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean shutdown() {
        try {
            if (persistenceManager != null) {
                persistenceManager.close();
                retentionExecutor.shutdown();
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private void performRetentionCleanup() {
        long cutoff = System.currentTimeMillis() - retentionMillis;
        rwLock.writeLock().lock();
        try {
            for (ConcurrentSkipListMap<Long, List<DataPoint>> metricMap : store.values()) {
                metricMap.headMap(cutoff, false).clear();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
        System.out.println("Retention cleanup completed at " + new Date());
    }

    private void insertWithoutLog(DataPoint dp) {
        insertWithoutLog(dp.getTimestamp(), dp.getMetric(), dp.getValue(), dp.getTags());
    }

    public void insertWithoutLog(long timestamp, String metric, double value, Map<String, String> tags) {
        long cutoff = System.currentTimeMillis() - retentionMillis;
        if (timestamp < cutoff) return;

        rwLock.writeLock().lock();
        try {
            DataPoint dp = new DataPoint(timestamp, metric, value, tags);
            store
                    .computeIfAbsent(dp.getMetric(), m -> new ConcurrentSkipListMap<>())
                    .computeIfAbsent(dp.getTimestamp(), t -> new ArrayList<>())
                    .add(dp);
        } finally {
            rwLock.writeLock().unlock();
        }
    }



}
