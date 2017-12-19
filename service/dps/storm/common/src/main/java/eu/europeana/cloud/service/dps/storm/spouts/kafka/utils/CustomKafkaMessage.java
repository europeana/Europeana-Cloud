package eu.europeana.cloud.service.dps.storm.spouts.kafka.utils;

import org.apache.storm.kafka.Partition;

/**
 * Created by Tarek on 11/27/2017.
 */
public class CustomKafkaMessage {
    private Partition partition;

    private long offset;

    public CustomKafkaMessage(Partition partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }


    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

}
