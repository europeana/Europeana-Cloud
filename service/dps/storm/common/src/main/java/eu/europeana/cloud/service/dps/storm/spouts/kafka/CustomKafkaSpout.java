package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.utils.CustomKafkaMessage;
import eu.europeana.cloud.service.dps.storm.utils.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Tarek on 11/27/2017.
 */
public class CustomKafkaSpout extends KafkaSpout {
    private SpoutConfig spoutConf;
    private String hosts;
    private int port;
    private String keyspaceName;
    private String userName;
    private String password;
    private CassandraConnectionProvider cassandraConnectionProvider;
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;
    private CassandraSubTaskInfoDAO cassandraSubTaskInfoDAO;
    private CassandraTaskErrorsDAO cassandraTaskErrorsDAO;

    public CustomKafkaSpout(SpoutConfig spoutConf, String hosts, int port, String keyspaceName,
                            String userName, String password) {

        super(spoutConf);
        this.spoutConf = spoutConf;
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;

    }

    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
        cassandraTaskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
        cassandraSubTaskInfoDAO = CassandraSubTaskInfoDAO.getInstance(cassandraConnectionProvider);
        cassandraTaskErrorsDAO = CassandraTaskErrorsDAO.getInstance(cassandraConnectionProvider);
    }

    @Override
    public void ack(Object msgId) {
        try {
            CustomKafkaMessage customKafkaMessage = buildCustomKafkaSpout(msgId);
            MessageAndOffset messageAndOffset = getMessageAndOffset(customKafkaMessage);
            Values tupleValues = getTupleValues(customKafkaMessage.getPartition(), messageAndOffset);
            DpsTask task = getDpsTask(tupleValues);
            long taskId = task.getTaskId();
            cassandraTaskInfoDAO.endTask(taskId, cassandraSubTaskInfoDAO.getProcessedFilesCount(taskId), cassandraTaskErrorsDAO.getErrorCount(taskId), "Completely processed", String.valueOf(TaskState.PROCESSED), new Date());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            super.ack(msgId);
        }
    }

    private DpsTask getDpsTask(Values values) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(values.get(0).toString(), DpsTask.class);
    }

    private Values getTupleValues(Partition partition, MessageAndOffset messageAndOffset) {
        Iterable<List<Object>> tuples = KafkaUtils.generateTuples(spoutConf, messageAndOffset.message(), partition.topic);
        Iterator<List<Object>> tuplesIterable = tuples.iterator();
        Values values = null;
        while (tuplesIterable.hasNext()) {
            values = (Values) tuplesIterable.next();
        }
        return values;
    }

    private MessageAndOffset getMessageAndOffset(CustomKafkaMessage customKafkaMessage) throws NoSuchFieldException, IllegalAccessException {
        PartitionCoordinator partitionCoordinator = getPartitionCoordinator();
        PartitionManager partitionManager = partitionCoordinator.getManager(customKafkaMessage.getPartition());
        SimpleConsumer simpleConsumer = getSimpleConsumer(partitionManager);
        ByteBufferMessageSet byteBufferMessageSet = KafkaUtils.fetchMessages(spoutConf, simpleConsumer, customKafkaMessage.getPartition(), customKafkaMessage.getOffset());
        Iterator<MessageAndOffset> messageAndOffsetIterable = byteBufferMessageSet.iterator();
        MessageAndOffset messageAndOffset = null;
        while (messageAndOffsetIterable.hasNext()) {
            messageAndOffset = messageAndOffsetIterable.next();
        }
        return messageAndOffset;
    }

    private SimpleConsumer getSimpleConsumer(PartitionManager partitionManager) throws NoSuchFieldException, IllegalAccessException {
        Field simpleConsumerField = partitionManager.getClass().getDeclaredField("_consumer");
        simpleConsumerField.setAccessible(true);
        return (SimpleConsumer) simpleConsumerField.get(partitionManager);
    }

    private PartitionCoordinator getPartitionCoordinator() throws NoSuchFieldException, IllegalAccessException {
        Field f = KafkaSpout.class.getDeclaredField("_coordinator");
        f.setAccessible(true);
        return (PartitionCoordinator) f.get(this);
    }

    @Override
    public void fail(Object msgId) {
        try {
            CustomKafkaMessage customKafkaMessage = buildCustomKafkaSpout(msgId);
            MessageAndOffset messageAndOffset = getMessageAndOffset(customKafkaMessage);
            Values tupleValues = getTupleValues(customKafkaMessage.getPartition(), messageAndOffset);
            DpsTask task = getDpsTask(tupleValues);
            long taskId = task.getTaskId();
            cassandraTaskInfoDAO.endTask(taskId, cassandraSubTaskInfoDAO.getProcessedFilesCount(taskId), cassandraTaskErrorsDAO.getErrorCount(taskId),  "The task was finished without a guarantee of complete processing", String.valueOf(TaskState.PROCESSED), new Date());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            super.ack(msgId);
        }
    }

    private CustomKafkaMessage buildCustomKafkaSpout(Object msgId) throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = msgId.getClass();
        Field partitionField = clazz.getDeclaredField("partition");
        partitionField.setAccessible(true);
        Partition partition = (Partition) partitionField.get(clazz.cast(msgId));
        Field offsetField = clazz.getDeclaredField("offset");
        offsetField.setAccessible(true);
        long offset = (Long) offsetField.get(clazz.cast(msgId));
        return new CustomKafkaMessage(partition, offset);
    }
}
