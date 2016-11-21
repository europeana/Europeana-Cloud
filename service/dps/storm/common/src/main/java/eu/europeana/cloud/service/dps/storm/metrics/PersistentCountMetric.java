package eu.europeana.cloud.service.dps.storm.metrics;


import org.apache.storm.metric.api.IMetric;

/**
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