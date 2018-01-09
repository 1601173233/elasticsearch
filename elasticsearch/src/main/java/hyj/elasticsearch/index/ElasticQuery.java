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
 * @Description: Es查询类
 * @author hyj     
 * @date 2018年1月9日 下午9:24:54  
 * @version V1.0
 */
public class ElasticQuery {
	/**
	 * 根据id查询结果
	 * @param index
	 * @param type
	 * @return
	 * @throws UnknownHostException
	 */
	public static Map<String, Object> getById(String index, String type, String id) throws UnknownHostException {
		TransportClient client = ElasticUtil.createClient();
		
		GetResponse response = client.prepareGet(index, type, id).get();
	
		client.close();
		
		//如果数据存在，那么返回数据
		if(response.isExists()) {
			return response.getSource();
		}else {
			//否则返回空值
			return null;
		}
	}

	public static void main(String[] args) throws UnknownHostException {
		String json = "{" +
				"\"user\":\"hyj\"," +
				"\"postDate\":\"2018-01-30\"," +
				"\"message\":\"trying out Elasticsearch\"" +
				"}";
		
		System.out.println(getById("text", "text", "2"));

	}

}