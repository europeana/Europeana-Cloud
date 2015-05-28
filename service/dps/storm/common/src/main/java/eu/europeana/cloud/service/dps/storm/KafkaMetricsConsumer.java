package eu.europeana.cloud.service.dps.storm;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

/*
 * Listens for, sends them to the proper Kafka topic.
 *
 */
public class KafkaMetricsConsumer implements IMetricsConsumer {

	public static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsConsumer.class);

	public static final String KAFKA_TOPIC_KEY = "kafka.topic";
	public static final String KAFKA_BROKER_KEY = "kafka.broker";

	private static String padding = "                       ";

	private String kafkaTopic;
	private String kafkaBroker;

	private Producer<String, String> p;

	@Override
	public void prepare(Map stormConf, Object registrationArgument,
			TopologyContext context, IErrorReporter errorReporter) {

		parseConfig(stormConf);
	}

	private void parseConfig(@SuppressWarnings("rawtypes") Map conf) {

		// TODO fetching properties from Conf does not work right now
		
//		if (conf.containsKey(KafkaMetricsConsumer.KAFKA_TOPIC_KEY)) {
//			kafkaTopic = (String) conf.get(KafkaMetricsConsumer.KAFKA_TOPIC_KEY);
//		} else {
//			Iterator i = conf.keySet().iterator();
//			while(i.hasNext()) {
//				Object n = conf.get(i.next());
//				if (n instanceof String) {
//					LOG.error((String)n);
//				}
//				else if (n instanceof Integer) {
//				}
//			}
//			LOG.error("KAFKA_TOPIC not found");
//		}
//
//		if (conf.containsKey(KafkaMetricsConsumer.KAFKA_BROKER_KEY)) {
//			kafkaBroker = (String) conf.get(KafkaMetricsConsumer.KAFKA_BROKER_KEY);
//		} else {
//			Iterator i = conf.keySet().iterator();
//			while(i.hasNext()) {
//				Object n = conf.get(i.next());
//				if (n instanceof String) {
//					LOG.error((String)n);
//				}
//			}
//			LOG.error("KAFKA_BROKER not found");
//		}
		
		// TODO hardcoded Kafka properties
		kafkaTopic = "storm_metrics_topic";
		//kafkaBroker = "ecloud.eanadev.org:9093";
		kafkaBroker = "192.168.47.129:9093";

		Properties props = new Properties();
		props.put("metadata.broker.list", kafkaBroker);
		props.put("serializer.class", "eu.europeana.cloud.service.dps.storm.JsonEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		this.p = new Producer<String, String>(config);
	}

	@Override
	public void handleDataPoints(TaskInfo taskInfo,
			Collection<DataPoint> dataPoints) {
		
		for (Metric metric : dataPointsToMetrics(taskInfo, dataPoints)) {
			report(metric.name, metric.value);
		}
	}

	private List<Metric> dataPointsToMetrics(TaskInfo taskInfo,
			Collection<DataPoint> dataPoints) {
		
		List<Metric> res = new LinkedList<>();

		StringBuilder sb = new StringBuilder()
				.append(clean(taskInfo.srcWorkerHost)).append(".")
				.append(taskInfo.srcWorkerPort).append(".")
				.append(clean(taskInfo.srcComponentId)).append(".");

		int hdrLength = sb.length();

		for (DataPoint p : dataPoints) {

			sb.delete(hdrLength, sb.length());
			sb.append(clean(p.name));

			if (p.value instanceof Number) {
				res.add(new Metric(sb.toString(), ((Number) p.value).intValue()));
			} else if (p.value instanceof Map) {
				int hdrAndNameLength = sb.length();
				@SuppressWarnings("rawtypes")
				Map map = (Map) p.value;
				for (Object subName : map.keySet()) {
					Object subValue = map.get(subName);
					if (subValue instanceof Number) {
						sb.delete(hdrAndNameLength, sb.length());
						sb.append(".").append(clean(subName.toString()));

						res.add(new Metric(sb.toString(), ((Number) subValue)
								.intValue()));
					}
				}
			}
		}
		return res;
	}
	
	private String clean(String s) {
		return s.replace('.', '_').replace('/', '_');
	}


	public void report(String s, int number) {
		
		LOG.info("Kafka: reporting: {}={}", s, number);

		String kafkaMessage = s + number;
		String key = "";
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(
				kafkaTopic, key, kafkaMessage);
		p.send(data);
	}

	@Override
	public void cleanup() {
		p.close();
	}

	public static class Metric {
		String name;
		int value;

		public Metric(String name, int value) {
			this.name = name;
			this.value = value;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Metric other = (Metric) obj;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (value != other.value)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "KafkaMetric [name=" + name + ", value=" + value + "]";
		}
	}
}
