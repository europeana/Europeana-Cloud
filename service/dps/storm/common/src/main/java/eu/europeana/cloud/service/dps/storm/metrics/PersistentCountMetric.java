package eu.europeana.cloud.service.dps.storm.metrics;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.IMetric;

/**
 * Same as {@link CountMetric} but does not reset when value is retrieved.
 * Useful to count statistics over a period of a long-running task.
 * 
 * @author manos
 */
public class PersistentCountMetric implements IMetric {
	
    long value = 0;

    public PersistentCountMetric() {
    }
    
    public void incr() {
    	value++;
    }

    public void incrBy(long incrementBy) {
    	value += incrementBy;
    }

    public Object getValueAndReset() {
        long ret = value;
        return ret;
    }
}