package eu.europeana.cloud.tools.statistics;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class TaskStatisticsAccumulator {

    private LocalDateTime startTime = null;
    private LocalDateTime previousOpTime = null;
    private final Statistic statistics = new Statistic();
    private final Map<String, Line> linesByKey = new HashMap<>();
    private Map<Integer, Duration> durations = new HashMap<>();
    private int openedCounter = 0;

    public TaskStatisticsAccumulator() {
    }
    public TaskStatisticsAccumulator(Line line) {
        process(line);
    }

    public TaskStatisticsAccumulator process(Line line) {
        var firstLine = (startTime == null);
        if (firstLine) {
            startTime = line.getDateTime();
            previousOpTime = startTime;
        }

        updateAverageData(line);
        if (line.isStartLine()) {
            updateOpenedNumber(+1, firstLine);
            linesByKey.put(line.getKey(), line);
        } else {
            if (linesByKey.containsKey(line.getKey())) {
                updateOpenedNumber(-1, firstLine);
                linesByKey.remove(line.getKey());
            } else {
                updateForNonOpened(firstLine);
            }
        }
        countAverage(line);

        return this;
    }

    private void updateOpenedNumber(int delta, boolean firstLine) {
        openedCounter += delta;
        statistics.checkExtremes(firstLine, openedCounter);
    }

    private void updateForNonOpened(boolean firstLine) {
        statistics.increaseExtremesByOne(firstLine);

        var tmpNewDurations = new HashMap<Integer, Duration>(durations.size());
        durations.forEach((key, value) -> tmpNewDurations.put(key + 1, value) );
        durations = tmpNewDurations;
    }

    private void updateAverageData(Line line) {
        var lastDuration = Duration.between(previousOpTime, line.getDateTime());
        this.previousOpTime = line.getDateTime();
        durations.compute(openedCounter, (key, value) -> value == null ? lastDuration : value.plus(lastDuration));
    }

    private void countAverage(Line line) {
        var timeFromBeginning = Duration.between(startTime, line.getDateTime()).toMillis();
        if (timeFromBeginning > 0) {
            long normalizedCounterSum = 0;
            for (Map.Entry<Integer, Duration> entry : durations.entrySet()) {
                normalizedCounterSum += entry.getKey() * entry.getValue().toMillis();
            }
            statistics.setAvg((float) (normalizedCounterSum / (double)timeFromBeginning));
        }
    }

    Statistic toStatistics() {
        return statistics;
    }
}
