package com.interview.timeseries;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;

/**
 * Manages write-ahead logging and replay of time series data.
 */
public class PersistenceManager {
    private final File logFile;
    private BufferedWriter writer;
    private static final long MAX_LOG_SIZE = 50 * 1024 * 1024; // 50 MB

    /**
     * Opens or creates the log file for appending.
     * @param filePath path to log file
     */
    public PersistenceManager(String filePath) throws IOException {
        this.logFile = new File(filePath);
        // Ensure parent directories exist
        File parent = logFile.getParentFile();
        if (parent != null) parent.mkdirs();
        this.writer = new BufferedWriter(new FileWriter(logFile, true));
    }

    /**
     * Appends a data point to the log.
     */
    public void append(DataPoint dp) throws IOException {
        rotateIfNeeded();
        StringBuilder sb = new StringBuilder();
        sb.append(dp.getTimestamp()).append(",");
        sb.append(escape(dp.getMetric())).append(",");
        sb.append(dp.getValue()).append(",");
        Map<String, String> tags = dp.getTags();
        boolean first = true;
        for (Map.Entry<String, String> e : tags.entrySet()) {
            if (!first) sb.append(";");
            first = false;
            sb.append(escape(e.getKey())).append("=").append(escape(e.getValue()));
        }
        writer.write(sb.toString());
        writer.newLine();
        writer.flush();
    }

    /**
     * Reads and parses all entries from the log file.
     */
    public void replayStreaming(Consumer<DataPoint> handler, long cutoffTimestamp) throws IOException {
        if (!logFile.exists()) return;

        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    DataPoint dp = parseLine(line);
                    if (dp.getTimestamp() >= cutoffTimestamp) {
                        handler.accept(dp);
                    }
                } catch (Exception e) {
                    System.err.println("Skipping invalid line: " + line);
                }
            }
        }
    }


    /**
     * Closes the log file for further writes.
     */
    public void close() throws IOException {
        writer.close();
    }

    // Escape commas, semicolons, equals and backslashes
    private String escape(String s) {
        return s.replace("\\", "\\\\").replace(",", "\\,")
                .replace(";", "\\;").replace("=", "\\=");
    }

    // Unescape sequences
    private String unescape(String s) {
        StringBuilder sb = new StringBuilder();
        boolean esc = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (esc) {
                sb.append(c);
                esc = false;
            } else if (c == '\\') {
                esc = true;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private void rotateIfNeeded() throws IOException {
        if (logFile.length() >= MAX_LOG_SIZE) {
            writer.close();
            File rotated = new File(logFile.getParent(), "timeseries_" + System.currentTimeMillis() + ".log");
            logFile.renameTo(rotated);
            writer = new BufferedWriter(new FileWriter(logFile, false)); // start fresh
        }
    }

    private DataPoint parseLine(String line) throws Exception {
        String[] parts = line.split(",", 4);
        if (parts.length < 4) throw new IllegalArgumentException("Invalid log line");

        long ts = Long.parseLong(parts[0]);
        String metric = unescape(parts[1]);
        double value = Double.parseDouble(parts[2]);

        Map<String, String> tags = new HashMap<>();
        if (!parts[3].isEmpty()) {
            for (String tag : parts[3].split(";")) {
                String[] kv = tag.split("=", 2);
                if (kv.length == 2) {
                    tags.put(unescape(kv[0]), unescape(kv[1]));
                }
            }
        }

        return new DataPoint(ts, metric, value, tags);
    }


}