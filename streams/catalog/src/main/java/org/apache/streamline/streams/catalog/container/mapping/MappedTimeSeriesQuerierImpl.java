package org.apache.streamline.streams.catalog.container.mapping;

public enum MappedTimeSeriesQuerierImpl {
    STORM_AMBARI_METRICS("org.apache.streamline.streams.metrics.storm.ambari.AmbariMetricsServiceWithStormQuerier");

    private final String className;

    MappedTimeSeriesQuerierImpl(String className) {
        this.className = className;
    }

    public String getClassName() {
        return className;
    }

    public static String getName(String streamingEngine, String timeSeriesDB) {
        return streamingEngine + "_" + timeSeriesDB;
    }
}