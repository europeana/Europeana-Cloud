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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomKafkaSpout.class);

    private SpoutConfig spoutConf;
    private String hosts;
    private int port;
    private String keyspaceName;
    private String userName;
    private String password;
    protected CassandraTaskInfoDAO cassandraTaskInfoDAO;
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
        LOGGER.info("Custom spout opened");
        super.open(conf, context, collector);
        CassandraConnectionProvider cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
        cassandraTaskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
        cassandraSubTaskInfoDAO = CassandraSubTaskInfoDAO.getInstance(cassandraConnectionProvider);
        cassandraTaskErrorsDAO = CassandraTaskErrorsDAO.getInstance(cassandraConnectionProvider);
    }

    @Override
    public void ack(Object msgId) {
        LOGGER.info("Message acknowledgement fired");
        try {
            CustomKafkaMessage customKafkaMessage = buildCustomKafkaMessage(msgId);
            MessageAndOffset messageAndOffset = getMessageAndOffset(customKafkaMessage);
            Values tupleValues = getTupleValues(customKafkaMessage.getPartition(), messageAndOffset);
            DpsTask task = getDpsTask(tupleValues);
            LOGGER.info("Acknowledgement fired for task: {}", task);
            long taskId = task.getTaskId();
            String state = String.valueOf(TaskState.PROCESSED);
            String infoMessage = "Completely processed";
            if (cassandraTaskInfoDAO.hasKillFlag(taskId)) {
                state = String.valueOf(TaskState.DROPPED);
                infoMessage = "Dropped by the user";
            }
            cassandraTaskInfoDAO.endTask(taskId, cassandraSubTaskInfoDAO.getProcessedFilesCount(taskId), cassandraTaskErrorsDAO.getErrorCount(taskId), infoMessage, state, new Date());
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
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
            if (messageAndOffset.offset() == customKafkaMessage.getOffset()) {
                return messageAndOffset;
            }
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
        LOGGER.info("Message fail method fired");
        try {
            CustomKafkaMessage customKafkaMessage = buildCustomKafkaMessage(msgId);
            MessageAndOffset messageAndOffset = getMessageAndOffset(customKafkaMessage);
            Values tupleValues = getTupleValues(customKafkaMessage.getPartition(), messageAndOffset);
            DpsTask task = getDpsTask(tupleValues);
            long taskId = task.getTaskId();
            LOGGER.info("Failed methos fired for task: {}", task.getTaskId());
            cassandraTaskInfoDAO.endTask(taskId, cassandraSubTaskInfoDAO.getProcessedFilesCount(taskId), cassandraTaskErrorsDAO.getErrorCount(taskId), "The task was finished without a guarantee of complete processing", String.valueOf(TaskState.PROCESSED), new Date());

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        } finally {
            super.ack(msgId);
        }
    }

    private CustomKafkaMessage buildCustomKafkaMessage(Object msgId) throws NoSuchFieldException, IllegalAccessException {
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
