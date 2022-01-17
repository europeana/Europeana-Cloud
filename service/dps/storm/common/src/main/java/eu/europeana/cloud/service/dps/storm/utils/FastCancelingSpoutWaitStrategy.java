package eu.europeana.cloud.service.dps.storm.utils;

import org.apache.storm.policy.IWaitStrategy;

import java.util.Map;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.SPOUT_SLEEP_MS;

public class FastCancelingSpoutWaitStrategy implements IWaitStrategy {

    private long sleepMs;
    private int sleepEveryNIdleIterations;

    @Override
    public void prepare(Map<String, Object> config, WaitSituation waitSituation) {
        sleepMs = ((Number) config.get(SPOUT_SLEEP_MS)).longValue();
        sleepEveryNIdleIterations = ((Number) config.get(SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS)).intValue();
    }

    @Override
    public int idle(int idleCounter) throws InterruptedException {
        if (++idleCounter % sleepEveryNIdleIterations == 0) {
            Thread.sleep(sleepMs);
        }
        return idleCounter;
    }
}
