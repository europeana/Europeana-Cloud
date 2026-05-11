package eu.europeana.cloud.service.dps.storm.utils;

import lombok.Getter;

@Getter
public class ExpectedCounters {
    /**
     * Private helper class to track expected record counts.
     */
    private int expectedRecords;
    private int expectedDepublishRecords;

    public ExpectedCounters(int expectedRecords, int expectedDepublishRecords) {
        this.expectedRecords = expectedRecords;
        this.expectedDepublishRecords = expectedDepublishRecords;
    }

    public static ExpectedCounters expectSingleRecord() {
        return new ExpectedCounters(1, 0);
    }

    public static ExpectedCounters expectSingleDepublishRecord() {
        return new ExpectedCounters(0, 1);
    }

    public static ExpectedCounters expectZeroRecords() {
        return new ExpectedCounters(0, 0);
    }

    /**
     * Add another Counters instance values into this one values.
     */
    public void add(ExpectedCounters other) {
        if (other == null) {
            return;
        }
        this.expectedRecords += other.expectedRecords;
        this.expectedDepublishRecords += other.expectedDepublishRecords;
    }

    public boolean areEmpty() {
        return this.expectedRecords == 0 && this.expectedDepublishRecords == 0;
    }
}
