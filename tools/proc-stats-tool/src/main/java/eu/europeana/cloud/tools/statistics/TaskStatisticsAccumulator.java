package eu.europeana.cloud.tools.statistics;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Accumulator realise inc/dec algorithm of opened connections Additionally it stores summary time (durations) for given number of
 * opened connections (for average calculating) Additionally accumulator recognizes situation that first info about some operation
 * is ending operation [END] so it increases number of opened connections by one from the beginning of analysing to found [END]
 * line
 */
public class TaskStatisticsAccumulator {

  /**
   * Store time information of first line in processed file
   */
  private LocalDateTime startTime = null;

  /**
   * Store time information of previous operation
   */
  private LocalDateTime previousOpTime = null;

  /**
   * Temporary information about beginning line
   */
  private final Map<String, LogLine> logLinesByKey = new HashMap<>();

  /**
   * Map keeps processing time for given number of opened connection Opened connections is a kay to map Summary time (stored as
   * duration) is a value for given key This map is needed only for counting average value
   */
  private Map<Integer, Duration> durations = new HashMap<>();

  /**
   * Opened connections counter. It is needed for extremes
   */
  private int openedCounter = 0;

  /**
   * Structure for storing statistics information
   */
  private final Statistic statistics = new Statistic();

  public TaskStatisticsAccumulator() {
  }

  public TaskStatisticsAccumulator(LogLine line) {
    process(line);
  }

  public TaskStatisticsAccumulator process(LogLine line) {
    //check if it is firs line in analysed file
    var firstProcessingLine = (startTime == null);

    //if yes, this is beginning time of analysing
    if (firstProcessingLine) {
      startTime = line.getDateTime();
      previousOpTime = startTime;
    }

    //store information for statistics (without calculating)
    updateAverageData(line);

    //Opening line about IO operation
    if (line.isOperationBegin()) {
      updateOpenedNumber(+1, firstProcessingLine);
      logLinesByKey.put(line.getKey(), line);
    } else {
      //Closing line about IO operation

      if (logLinesByKey.containsKey(line.getKey())) {
        //as a pair of opened operation earlier
        updateOpenedNumber(-1, firstProcessingLine);
        logLinesByKey.remove(line.getKey());
      } else {
        //closing line for unopened operation
        updateForNonOpened(firstProcessingLine);
      }
    }

    //calculate average value
    countAverage(line);

    //return this as value for functional call only
    return this;
  }

  /**
   * Update (+1/-1) opened connections counter and calculate new extremes for statistics
   *
   * @param delta Increase/decrease value
   * @param firstLine Flag if first line from file is processing
   */
  private void updateOpenedNumber(int delta, boolean firstLine) {
    openedCounter += delta;
    statistics.checkExtremes(firstLine, openedCounter);
  }

  private void updateForNonOpened(boolean firstLine) {
    //non opened means that something was opened before lines analysing - so increase statistics by one
    statistics.increaseExtremesByOne(firstLine);

    //Durations map for every key (number of opened connections) has to be increased.
    // But the key is increased not time!
    var tmpNewDurations = new HashMap<Integer, Duration>(durations.size());
    durations.forEach((key, value) -> tmpNewDurations.put(key + 1, value));
    durations = tmpNewDurations;
  }

  private void updateAverageData(LogLine line) {
    //calculate time (as duration) from previous change of the openedCounter
    var lastDuration = Duration.between(previousOpTime, line.getDateTime());

    //store current time as previous one
    this.previousOpTime = line.getDateTime();

    //add calculated duration to duration stored for given number of connections
    durations.compute(openedCounter, (key, value) -> value == null ? lastDuration : value.plus(lastDuration));
  }


  /**
   * Count average value of opened connections normalized by processing time 5 connections by 100s is not this same like 5
   * connection by 10s! Result is stored in statistics structure
   *
   * @param line Current processed line. Rest of data is retrieved form startTime and durations fields
   */
  private void countAverage(LogLine line) {
    //Count time from first line to current one
    var timeFromBeginning = Duration.between(startTime, line.getDateTime()).toMillis();

    //if this time is bigger than 0
    if (timeFromBeginning > 0) {
      long normalizedCounterSum = 0;

      //summarize opened connections (key) multiply by appropriate durations (in ms)
      for (Map.Entry<Integer, Duration> entry : durations.entrySet()) {
        normalizedCounterSum += entry.getKey() * entry.getValue().toMillis();
      }

      //and divide it by time from beginning
      statistics.setAvg((float) (normalizedCounterSum / (double) timeFromBeginning));
    }
  }

  Statistic toStatistics() {
    return statistics;
  }
}
