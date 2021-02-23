package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import org.apache.storm.spout.ISpoutWaitStrategy;

import java.util.Map;

public class FastSleepSpoutWaitStrategy implements ISpoutWaitStrategy {

    @Override
    public void prepare(Map conf) {
    }

    @Override
    public void emptyEmit(long streak) {
        try {
            if((streak%20) == 19) {
                Thread.sleep(1L);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}