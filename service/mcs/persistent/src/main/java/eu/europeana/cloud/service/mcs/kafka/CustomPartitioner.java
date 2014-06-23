package eu.europeana.cloud.service.mcs.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Routing messages to different partitions via routingKey.
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
     * Routing messages to different partitions via routingKey.
     * 
     * @param routingKey
     *            used to route message to partitions
     * @param numPartitions
     *            number of partitions
     * @return number of routed partition
     */
    @Override
    public int partition(Object routingKey, int numPartitions) {
	if (routingKey instanceof String) {
	    return partition((String) routingKey, numPartitions);
	}
	throw new RuntimeException("Unsuppored as parttition key");
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
    public int partition(String routingKey, int numPartitions) {
	final int hash = Integer.valueOf(routingKey);
	return hash % numPartitions;
    }
}
