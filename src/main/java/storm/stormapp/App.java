package storm.stormapp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutStreamsNamedTopics;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilderNamedTopics;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * @Title: App
 * @Package: storm.stormapp
 * @author: zengxinchao
 * @date: 2017年1月22日 上午10:12:54
 * @Description: App
 */
public class App {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		String[] topics = new String[] { "test" };
		Fields outputFields = new Fields("topic", "partition", "offset", "key", "value");
		KafkaSpoutTuplesBuilder<String, String> tuplesBuilder = new KafkaSpoutTuplesBuilderNamedTopics.Builder<String, String>(new KafkaSpoutTupleBuilder<String, String>("test") {
			private static final long serialVersionUID = -2894314795576512299L;
			
			@Override
			public List<Object> buildTuple(ConsumerRecord<String, String> consumerRecord) {
				return new Values(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
			}
		}).build();
		KafkaSpoutStreams kafkaSpoutStreams = new KafkaSpoutStreamsNamedTopics.Builder(outputFields, "stream", topics).build();
		
		Map<String, Object> kafkaConsumerProps = new HashMap<String, Object>();
		kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, "117.25.133.91:9092,117.25.133.91:9093");
		kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.GROUP_ID, "storm");
		kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaSpoutRetryService retryService = new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
				KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
		KafkaSpoutConfig<String, String> kafkaSpoutConfig = new KafkaSpoutConfig.Builder<String, String>(kafkaConsumerProps, kafkaSpoutStreams, tuplesBuilder, retryService)
				.setOffsetCommitPeriodMs(10000).setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST).setMaxUncommittedOffsets(50000).build();
		KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>(kafkaSpoutConfig);
		
		TopologyBuilder tp = new TopologyBuilder();
		tp.setSpout("nginxreader", kafkaSpout, 1);
		tp.setBolt("dataprocessor", new KafkaNginxLogProcessor(), 3).shuffleGrouping("nginxreader", "stream");
		
		Config conf = new Config();
		conf.setNumWorkers(1);
		conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, false);
		// conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1024);
		// conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
		// conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		// conf.put(Config.BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK, 0.8);
		// conf.put(Config.BACKPRESSURE_DISRUPTOR_LOW_WATERMARK, 0.4);
		// conf.setDebug(true);
		// LocalCluster cluster = new LocalCluster();
		// cluster.submitTopology("TEST", conf, tp.createTopology());
		StormSubmitter.submitTopology("nginx_log", conf, tp.createTopology());
	}
}
