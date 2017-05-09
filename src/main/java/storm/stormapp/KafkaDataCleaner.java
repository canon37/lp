package storm.stormapp;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDataCleaner extends BaseRichBolt {
	private static final long serialVersionUID = -1041342737125400417L;
	
	private OutputCollector collector;
	
	public static final Logger log = LoggerFactory.getLogger(KafkaDataProcessor.class);
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void execute(Tuple input) {
		String line = input.getStringByField("value");
		String[] elements = line.split(" ");
		collector.emit(input, new Values(elements[0] + "-" + elements[1], " test ", new Integer(elements[2])));
		collector.ack(input);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "test", "random"));
	}
	
	public OutputCollector getCollector() {
		return collector;
	}
	
	public void setCollector(OutputCollector collector) {
		this.collector = collector;
	}
	
}
