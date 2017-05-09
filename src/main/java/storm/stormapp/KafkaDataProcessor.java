package storm.stormapp;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDataProcessor extends BaseRichBolt {
	public static final long	serialVersionUID	= -6686777289203653770L;
	
	private static final Logger	log					= LoggerFactory.getLogger(KafkaDataProcessor.class);
	
	private OutputCollector		collector;
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void execute(Tuple input) {
		String date = input.getStringByField("time");
		Integer r = input.getIntegerByField("random");
		collector.ack(input);
		log.info("------------at " + date + " the random is " + r + "---------------");
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	public OutputCollector getCollector() {
		return collector;
	}
	
	public void setCollector(OutputCollector collector) {
		this.collector = collector;
	}
	
}
