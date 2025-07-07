package com.interview.timeseries;

import java.io.*;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Simple main class for manual testing of the TimeSeriesStore implementation.
 */
public class Main {

    public static void main(String[] args) {
        TimeSeriesStoreImpl store = new TimeSeriesStoreImpl();
        boolean initialized = store.initialize();
        System.out.println("Store initialized: " + initialized);

        if (!initialized) {
            System.err.println("Failed to initialize the store. Exiting.");
            return;
        }

        try {
            String csvPath = Paths.get("time_series_data.csv").toAbsolutePath().toString();
            System.out.println("Loading data from: " + csvPath);
            try (BufferedReader reader = new BufferedReader(new FileReader(csvPath))) {
                String headerLine = reader.readLine(); // read headers
                if (headerLine == null) {
                    System.err.println("CSV file is empty.");
                    return;
                }

                // ✅ Move headers declaration outside the loop
                String[] headers = headerLine.split(",");
                if (headers.length < 4) {
                    System.err.println("Insufficient columns in CSV.");
                    return;
                }

                String line;
                int insertedCount = 0;
                int skippedOld = 0;
                int malformed = 0;

                long cutoff = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(24);

                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",", -1); // keep empty strings
                    if (fields.length != headers.length) {
                        malformed++;
                        continue;
                    }

                    try {
                        long timestamp = Long.parseLong(fields[0]);
                        if (timestamp < cutoff) {
                            skippedOld++;
                            continue;
                        }

                        String metric = fields[1];
                        double value = Double.parseDouble(fields[2]);

                        Map<String, String> tags = new HashMap<>();
                        for (int i = 3; i < headers.length; i++) {
                            tags.put(headers[i], fields[i]);
                        }

                        // Call insertWithoutLog to skip re-logging
                        TimeSeriesStoreImpl impl = store;
                        impl.insertWithoutLog(timestamp, metric, value, tags);
                        insertedCount++;

                        if (insertedCount % 100_000 == 0) {
                            System.out.println("Inserted: " + insertedCount + " records...");
                        }

                    } catch (Exception e) {
                        malformed++;
                        System.err.println("Skipping invalid row: " + line);
                    }
                }

                System.out.println("Finished loading.");
                System.out.println("Inserted: " + insertedCount);
                System.out.println("Skipped old: " + skippedOld);
                System.out.println("Malformed: " + malformed);

            }

            // Example query after loading
            long now = System.currentTimeMillis();
            List<DataPoint> recentCpu = store.query("cpu.usage", now - 3600_000, now, Map.of());
            System.out.println("Queried cpu.usage in last hour: " + recentCpu.size() + " records");

        } catch (IOException e) {
            System.err.println("Failed to load CSV: " + e.getMessage());
        } finally {
            boolean shutdown = store.shutdown();
            System.out.println("Store shutdown: " + shutdown);
        }
    }


    private static void loadDataFromCsv(TimeSeriesStore store, String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            int lineNum = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (line.trim().isEmpty()) continue;

                String[] parts = line.split(",", 4);
                if (parts.length < 4) {
                    System.err.println("Skipping malformed line " + lineNum + ": " + line);
                    continue;
                }

                try {
                    long timestamp = Long.parseLong(parts[0].trim());
                    String metric = parts[1].trim();
                    double value = Double.parseDouble(parts[2].trim());
                    Map<String, String> tags = parseTags(parts[3].trim());

                    store.insert(timestamp, metric, value, tags);
                } catch (Exception e) {
                    System.err.println("Error parsing line " + lineNum + ": " + line);
                    e.printStackTrace();
                }
            }
            System.out.println("CSV load complete.");
        } catch (IOException e) {
            System.err.println("Failed to read CSV file: " + filePath);
            e.printStackTrace();
        }
    }

    private static Map<String, String> parseTags(String tagString) {
        Map<String, String> tags = new HashMap<>();
        if (tagString.isEmpty()) return tags;

        String[] pairs = tagString.split(";");
        for (String pair : pairs) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                tags.put(kv[0].trim(), kv[1].trim());
            }
        }
        return tags;
    }
}
