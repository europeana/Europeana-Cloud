package eu.europeana.cloud.service.dps.storm.utils;

import com.google.common.base.Stopwatch;
import org.apache.storm.policy.IWaitStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.SPOUT_SLEEP_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FastCancelingSpoutWaitStrategyTest {

  private static final long TOLERANCE = 100L;
  public static final long SLEEP = 200L;
  private FastCancelingSpoutWaitStrategy strategy;

  @BeforeEach
  void setup() {
    strategy = new FastCancelingSpoutWaitStrategy();
    Map<String, Object> config = Map.of(SPOUT_SLEEP_MS, SLEEP, SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS, 3);
    strategy.prepare(config, IWaitStrategy.WaitSituation.SPOUT_WAIT);
  }

  @ParameterizedTest
  @MethodSource("sleepTestArguments")
  void shouldSleepAfterNIteration(int repeatCount, long expectedSleep) throws InterruptedException {
    Stopwatch watch = Stopwatch.createStarted();

    long elapsed = runWaitStrategyNTimes(watch, repeatCount);

    assertEquals(expectedSleep, elapsed, TOLERANCE);
  }


  static Stream<Arguments> sleepTestArguments() {
    return Stream.of(
            Arguments.of(3, 200L),
            Arguments.of(6, 400L),
            Arguments.of(2, 0L)
    );
  }

  @Test
  void shouldSleep2TimesAfterNx2Iteration() throws InterruptedException {
    Stopwatch watch = Stopwatch.createStarted();

    long elapsed = runWaitStrategyNTimes(watch, 6);

    assertEquals(400L, elapsed, TOLERANCE);
  }

  @Test
  void shouldNotSleepBeforeNIteration() throws InterruptedException {
    Stopwatch watch = Stopwatch.createStarted();

    long elapsed = runWaitStrategyNTimes(watch, 2);

    assertEquals(0L, elapsed, TOLERANCE);
  }

  private long runWaitStrategyNTimes(Stopwatch watch, int repeatCount) throws InterruptedException {
    int idleCount = 0;
    for (int i = 0; i < repeatCount; i++) {
      idleCount = strategy.idle(idleCount);
    }
    return watch.elapsed(TimeUnit.MILLISECONDS);


  }
}