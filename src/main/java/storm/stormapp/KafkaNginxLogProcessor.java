package storm.stormapp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zuipin.util.StringUtil;

/**
 * @Title: KafkaNginxLogProcessor
 * @Package: storm.stormapp
 * @author: zengxinchao
 * @date: 2017年1月22日 上午10:13:05
 * @Description: KafkaNginxLogProcessor
 */
public class KafkaNginxLogProcessor extends BaseRichBolt {
	private static final long	serialVersionUID	= -3572830893934548392L;
	
	private OutputCollector		collector;
	
	private static String		sql					= "insert into nginx_log(log_date,ip,user_agent,host_name,request,request_body) values(?,?,?,?,?,?)";
	
	private BasicDataSource		dataSource;
	
	public static final Logger	log					= LoggerFactory.getLogger(KafkaNginxLogProcessor.class);
	
	public static Pattern		pattern				= Pattern.compile("^[A-Za-z0-9_]{1,}.[a-zA-Z_]{1,}.(cn|com)$");
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		dataSource.setUrl("jdbc:mysql://192.168.1.129:3306/nginxlog?useUnicode=true&autoReconnect=true&characterEncoding=UTF-8");
		dataSource.setUsername("root");
		dataSource.setPassword("7+ve^05iA$");
		dataSource.setMaxWait(5);
		dataSource.setMaxActive(5);
		dataSource.setMaxIdle(3);
		dataSource.setTestWhileIdle(true);
		dataSource.setValidationQuery("select 1 from dual");
		this.collector = collector;
	}
	
	public void execute(Tuple input) {
		String line = input.getStringByField("value");
		if (StringUtil.isNotBlank(line)) {
			String[] elements = line.split("\"");
			if (elements.length < 12) {
				// log.info("tuple size not conform");
			} else {
				String date = elements[0].substring(0, 19);
				String ip = elements[0].split(" ")[2];
				String userAgent = elements[7];
				String hostName = elements[11];
				String request = elements[1];
				String requestBody = elements[5];
				
				if (filt(request, requestBody, ip, hostName, userAgent)) {
					if (requestBody.length() > 2000) {
						requestBody = requestBody.substring(0, 2000);
					}
					if (request.length() > 2000) {
						request = request.substring(0, 2000);
					}
					if (hostName.equals("m.oteao.com") && (requestBody.contains("teaplat") || request.contains("teaplat"))) {
						hostName = "teaplat";
					}
					
					try (Connection conn = dataSource.getConnection()) {
						PreparedStatement stmt = conn.prepareStatement(sql);
						stmt.setObject(1, date);
						stmt.setObject(2, ip);
						stmt.setObject(3, userAgent);
						stmt.setObject(4, hostName);
						stmt.setObject(5, request);
						stmt.setObject(6, requestBody);
						stmt.execute();
					} catch (Exception e) {
						log.error("", e);
						collector.fail(input);
					}
				} else {
					// redis
				}
			}
		} else {
			log.info("fetch a empty tuple");
		}
		collector.ack(input);
	}
	
	private boolean filt(String request, String requestBody, String ip, String hostName, String agent) {
		if (ip.startsWith("192.168") || ip.equals("59.57.240.242") || ip.equals("117.25.133.91")) {// 内网、公司ip
			return false;
		}
		if (request.contains(".jpg") || requestBody.contains(".jpg")) {
			return false;
		}
		if (request.contains(".js") || requestBody.contains(".js")) {
			return false;
		}
		if (request.contains(".css") || requestBody.contains(".css")) {
			return false;
		}
		if (request.contains(".png") || requestBody.contains(".png")) {
			return false;
		}
		if (request.contains(".tmp") || requestBody.contains(".tmp")) {
			return false;
		}
		if (request.contains(".gif") || requestBody.contains(".gif")) {
			return false;
		}
		if (!pattern.matcher(hostName).matches() || hostName.startsWith("img") || hostName.startsWith("xy") || hostName.startsWith("wx")) {
			return false;
		}
		if (agent.contains("spider") || agent.contains("Spider")) {
			return false;
		}
		return true;
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	public OutputCollector getCollector() {
		return collector;
	}
	
	public void setCollector(OutputCollector collector) {
		this.collector = collector;
	}
	
}
