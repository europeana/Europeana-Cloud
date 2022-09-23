package eu.europeana.cloud.tools.statistics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StatisticsAccumulator {
    private final Map<Long, TaskStatisticsAccumulator> taskStatistics = new HashMap<>();

    public void process(LogLine line) {
        taskStatistics.compute(line.getTaskId(),
                (key, value) -> value == null ? new TaskStatisticsAccumulator(line) : value.process(line) );
    }

    Map<Long, Statistic> toStatistics() {
        var tempResult = new HashMap<Long, Statistic>();
        taskStatistics.forEach((key, value) -> tempResult.put(key, value.toStatistics()));
        return Collections.unmodifiableMap(tempResult);
    }

}
