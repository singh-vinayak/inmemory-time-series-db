package com.interview.timeseries;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for the TimeSeriesStore implementation.
 */
public class TimeSeriesStoreTest {
    
    private TimeSeriesStore store;
    
    @Before
    public void setUp() {
        store = new TimeSeriesStoreImpl();
        store.initialize();
    }
    
    @After
    public void tearDown() {
        store.shutdown();
    }
    
    @Test
    public void testInsertAndQueryBasic() {
        // Insert test data
        long now = System.currentTimeMillis();
        Map<String, String> tags = new HashMap<>();
        tags.put("host", "server1");
        
        assertTrue(store.insert(now, "cpu.usage", 45.2, tags));
        
        // Query for the data
        List<DataPoint> results = store.query("cpu.usage", now, now + 1, tags);
        
        // Verify
        assertEquals(1, results.size());
        assertEquals(now, results.get(0).getTimestamp());
        assertEquals("cpu.usage", results.get(0).getMetric());
        assertEquals(45.2, results.get(0).getValue(), 0.001);
        assertEquals("server1", results.get(0).getTags().get("host"));
    }
    
    @Test
    public void testQueryTimeRange() {
        // Insert test data at different times
        long start = System.currentTimeMillis();
        Map<String, String> tags = new HashMap<>();
        tags.put("host", "server1");
        
        store.insert(start, "cpu.usage", 45.2, tags);
        store.insert(start + 1000, "cpu.usage", 48.3, tags);
        store.insert(start + 2000, "cpu.usage", 51.7, tags);
        
        // Query for a subset
        List<DataPoint> results = store.query("cpu.usage", start, start + 1500, tags);
        
        // Verify
        assertEquals(2, results.size());
    }
    
    @Test
    public void testQueryWithFilters() {
        // Insert test data with different tags
        long now = System.currentTimeMillis();
        
        Map<String, String> tags1 = new HashMap<>();
        tags1.put("host", "server1");
        tags1.put("datacenter", "us-west");
        
        Map<String, String> tags2 = new HashMap<>();
        tags2.put("host", "server2");
        tags2.put("datacenter", "us-west");
        
        store.insert(now, "cpu.usage", 45.2, tags1);
        store.insert(now, "cpu.usage", 42.1, tags2);
        
        // Query with filter on datacenter
        Map<String, String> filter = new HashMap<>();
        filter.put("datacenter", "us-west");
        
        List<DataPoint> results = store.query("cpu.usage", now, now + 1, filter);
        
        // Verify
        assertEquals(2, results.size());
        
        // Query with filter on host
        filter.clear();
        filter.put("host", "server1");
        
        results = store.query("cpu.usage", now, now + 1, filter);
        
        // Verify
        assertEquals(1, results.size());
        assertEquals("server1", results.get(0).getTags().get("host"));
    }

    @Test
    public void testMultipleMetricsIsolation() {
        long now = System.currentTimeMillis();
        store.insert(now, "cpu.usage", 50.0, Map.of("host", "server1"));
        store.insert(now, "memory.used", 80.0, Map.of("host", "server1"));

        List<DataPoint> cpuResults = store.query("cpu.usage", now, now + 1, Map.of());
        List<DataPoint> memResults = store.query("memory.used", now, now + 1, Map.of());

        assertEquals(1, cpuResults.size());
        assertEquals("cpu.usage", cpuResults.get(0).getMetric());

        assertEquals(1, memResults.size());
        assertEquals("memory.used", memResults.get(0).getMetric());
    }

    @Test
    public void testTagMismatch() {
        long now = System.currentTimeMillis();
        store.insert(now, "cpu.usage", 60.0, Map.of("host", "server1"));

        Map<String, String> mismatchFilter = Map.of("host", "serverX");
        List<DataPoint> result = store.query("cpu.usage", now, now + 1, mismatchFilter);

        assertEquals(0, result.size());
    }

    @Test
    public void testQueryWithoutFilters() {
        long now = System.currentTimeMillis();
        store.insert(now, "cpu.usage", 70.0, Map.of("region", "us-east"));
        store.insert(now, "cpu.usage", 75.0, Map.of("region", "us-west"));

        List<DataPoint> result = store.query("cpu.usage", now, now + 1, Map.of());

        assertEquals(2, result.size());
    }

    @Test
    public void testInsertDuplicateTimestamp() {
        long now = System.currentTimeMillis();
        store.insert(now, "cpu.usage", 88.8, Map.of("host", "a"));
        store.insert(now, "cpu.usage", 99.9, Map.of("host", "b"));

        List<DataPoint> results = store.query("cpu.usage", now, now + 1, Map.of());
        assertEquals(2, results.size());
    }

    @Test
    public void testPersistenceAndRecovery() {
        long now = System.currentTimeMillis();
        Map<String, String> tags = Map.of("host", "persistent");

        store.insert(now, "disk.io", 33.3, tags);

        // Simulate shutdown and restart
        store.shutdown();

        store = new TimeSeriesStoreImpl();
        store.initialize();

        List<DataPoint> results = store.query("disk.io", now, now + 1, tags);
        assertEquals(1, results.size());
        assertEquals("persistent", results.get(0).getTags().get("host"));
    }

}
