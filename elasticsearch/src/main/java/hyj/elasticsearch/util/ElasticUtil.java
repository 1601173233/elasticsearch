package hyj.elasticsearch.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
/**
 * es工具类
 * @since 2018年1月9日
 * @author hyj
 * @version 1.0
 */
public class ElasticUtil {
	/**
	 * 创建连接
	 * @return
	 * @throws UnknownHostException
	 */
	public static TransportClient createClient() throws UnknownHostException{
		return createClient(Contants.ELASTIC_CLUSTER_NAME, Contants.ELASTIC_SERVICE_IP, Contants.ELASTIC_SERVICE_PORT);
	}
		
	
	/**
	 * 创建连接
	 * @param clusterName 集群名字
	 * @param ip          ip地址
	 * @param port        端口号
	 * @return
	 * @throws UnknownHostException 
	 */
	public static TransportClient createClient(String clusterName, String ip, int port) throws UnknownHostException{
		// TODO Auto-generated method stub
		// 设置集群名称
		Settings settings = Settings.builder()
				.put("cluster.name", clusterName).build();

		// 创建client
		@SuppressWarnings("resource")
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(InetAddress
						.getByName(ip), port));
		
		return client;
	}
	
	/**
	 * 添加参数
	 * @param map
	 * @return 
	 * @throws IOException 
	 */
	public static XContentBuilder addFields1(Map<String, Object> map, XContentBuilder builder) throws IOException {
		Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
		
		while (it.hasNext()) {
			Map.Entry<String, Object> entry = it.next();
			builder = builder.field(entry.getKey(), entry.getValue());
		}
		
		return builder;
	}
}
