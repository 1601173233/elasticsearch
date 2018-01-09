package hyj.elasticsearch.index;

import hyj.elasticsearch.util.ElasticUtil;

import java.net.UnknownHostException;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.mapper.ObjectMapper;

public class ElasticIndex {
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param jsonDate 
	 * @throws UnknownHostException 
	 */
	public static void createIndex(String index, String type, String jsonDate) throws UnknownHostException {
		TransportClient client = ElasticUtil.createClient();
		
		IndexResponse response = client.prepareIndex(index, type)
					.setSource(jsonDate).get();
		
		client.close();
	}
	
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param jsonDate 
	 * @throws UnknownHostException 
	 */
	public static void createIndex(String index, String type, Object obj) throws UnknownHostException {
		TransportClient client = ElasticUtil.createClient();
		
		// instance a json mapper
		ObjectMapper mapper = new ObjectMapper(); // create once, re
		
		// generate json
		byte[] json = mapper.writeValueAsBytes(obj);
		
		IndexResponse response = client.prepareIndex(index, type)
					.setSource(jsonDate).get();
		
		client.close();
	}

	public static void main(String[] args) throws UnknownHostException {
		String json = "{" +
				"\"user\":\"kimchy\"," +
				"\"postDate\":\"2013-01-30\"," +
				"\"message\":\"trying out Elasticsearch\"" +
				"}";
		
		createIndex("text", "text", json);

	}

}