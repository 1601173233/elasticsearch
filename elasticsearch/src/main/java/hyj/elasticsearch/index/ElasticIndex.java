package hyj.elasticsearch.index;

import hyj.elasticsearch.util.ElasticUtil;

import java.net.UnknownHostException;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
/**
 * 
 * @Description: ES索引操作类
 * @author hyj     
 * @date 2018年1月9日 下午9:23:04  
 * @version V1.0
 */
public class ElasticIndex {
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param jsonDate 
	 * @throws UnknownHostException 
	 */
	public static void createIndex(String index, String type, Map<String, Object> map) throws UnknownHostException {
		TransportClient client = ElasticUtil.createClient();
		
		IndexResponse response = client.prepareIndex(index, type)
					.setSource(map).get();
		
		client.close();
	}
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param jsonDate 
	 * @throws UnknownHostException 
	 */
	public static void createIndex(String index, String type, String id, Map<String, Object> map) throws UnknownHostException {
		TransportClient client = ElasticUtil.createClient();
		
		IndexResponse response = client.prepareIndex(index, type, id)
					.setSource(map).get();
		
		client.close();
	}
	
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
	public static void createIndex(String index, String type, String id, String jsonDate) throws UnknownHostException {
		TransportClient client = ElasticUtil.createClient();
		
		IndexResponse response = client.prepareIndex(index, type, id)
					.setSource(jsonDate).get();
		
		client.close();
	}
	
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param obj      对象 
	 * @throws UnknownHostException 
	 * @throws JsonProcessingException 
	 */
	public static void createIndex(String index, String type, Object obj) throws UnknownHostException, JsonProcessingException {
		TransportClient client = ElasticUtil.createClient();
		
		// instance a json mapper
		ObjectMapper mapper = new ObjectMapper(); // create once, re
		
		// generate json
		byte[] json = mapper.writeValueAsBytes(obj);
		
		IndexResponse response = client.prepareIndex(index, type)
					.setSource(json).get();
		
		client.close();
	}
	
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param obj      对象 
	 * @throws UnknownHostException 
	 * @throws JsonProcessingException 
	 */
	public static void createIndex(String index, String type, String id, Object obj) throws UnknownHostException, JsonProcessingException {
		TransportClient client = ElasticUtil.createClient();
		
		// instance a json mapper
		ObjectMapper mapper = new ObjectMapper(); // create once, re
		
		// generate json
		byte[] json = mapper.writeValueAsBytes(obj);
		
		IndexResponse response = client.prepareIndex(index, type, id)
					.setSource(json).get();
		
		client.close();
	}

	public static void main(String[] args) throws UnknownHostException {
		String json = "{" +
				"\"id\":\"1\"," +
				"\"user\":\"hyj1\"," +
				"\"postDate\":\"2018-01-30\"," +
				"\"message\":\"trying out Elasticsearch\"" +
				"}";
		
		createIndex("text", "text", "2", json);

	}

}