package hyj.elasticsearch.index;

import hyj.elasticsearch.util.ElasticUtil;
import hyj.elasticsearch.util.XContentUtil;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
/**
 * 
 * @Description: ES索引操作类
 * @author hyj     
 * @date 2018年1月9日 下午9:23:04  
 * @version V1.0
 */
public class ElasticUpdate {
	/**
	 * 更新索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param id       索引的id
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void update(String index, String type, String id, Map<String, Object> map) throws IOException, InterruptedException, ExecutionException {
		TransportClient client = ElasticUtil.createClient();
		
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(index);
		updateRequest.type(type);
		updateRequest.id(id);
		
		//构造参数列
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
		builder = XContentUtil.addFields(map, builder);
		builder = builder.endObject();
		
		updateRequest.doc(builder);
		client.update(updateRequest).get();
		client.close();
	}
	
	/**
	 * 删除索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param id       索引的id
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void delete(String index, String type, String id) throws IOException, InterruptedException, ExecutionException {
		TransportClient client = ElasticUtil.createClient();
		
		DeleteResponse response = client.prepareDelete(index, type, id).get();
		client.close();
	}
	
	/**
	 * 根据条件删除索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param id       索引的id
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void deleteByQuery(String index, String type, String id) throws IOException, InterruptedException, ExecutionException {
		TransportClient client = ElasticUtil.createClient();
		
//		BulkByScrollResponse response =
//				DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
//				.filter(QueryBuilders.matchQuery("gender", "male")) //查
//				询条件
//				.source("persons") //index(索引名)
//				.get(); //执行
//				long deleted = response.getDeleted(); //删除文档的数量
		client.close();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", "hyj1111");
		map.put("age", "11111");
		
//		update("text", "text", "1", map);
		delete("text", "text", "2");

	}
}