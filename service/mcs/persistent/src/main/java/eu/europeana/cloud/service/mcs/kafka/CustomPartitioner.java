package eu.europeana.cloud.service.mcs.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Used to route messages to different partitions using routingKey.
 * 
 */
public class CustomPartitioner implements Partitioner {

    /**
     * Constructs a CustomizePartitioner.
     * 
     * @param props
     *            unused but required by Kafka.
     */
    public CustomPartitioner(VerifiableProperties props) {
    }


    /**
     * Returns partition id for given routing key and number of partitions.
     * 
     * @param routingKey
     *            used to route message to partitions
     * @param numPartitions
     *            number of partitions
     * @return partition id
     */
    @Override
    public int partition(Object routingKey, int numPartitions) {
        if (routingKey instanceof String) {
            return getPartitionIdFromStringRoutingKey((String) routingKey, numPartitions);
        }
        throw new RuntimeException("Unsuppored argument as parttition key");
    }


    /**
     * Routing messages to different partitions via routingKey.
     * 
     * @param routingKey
     *            used to route message to partitions
     * @param numPartitions
     *            number of partitions
     * @return number of routed partition
     */
    private int getPartitionIdFromStringRoutingKey(String routingKey, int numPartitions) {
        final int hash = Integer.valueOf(routingKey);
        return Math.abs(hash % numPartitions);
    }
}
