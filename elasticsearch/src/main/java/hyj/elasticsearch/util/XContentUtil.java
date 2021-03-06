package hyj.elasticsearch.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
/**
 * 
 * @Description: XContentUtil的操作类
 * @author hyj     
 * @date 2018年1月9日 下午11:09:31  
 * @version V1.0
 */
public class XContentUtil {
	/**
	 * 添加参数
	 * @param map
	 * @return 
	 * @throws IOException 
	 */
	public static XContentBuilder addFields(Map<String, Object> map, XContentBuilder builder) throws IOException {
		Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
		
		while (it.hasNext()) {
			Map.Entry<String, Object> entry = it.next();
			if(entry.getValue().getClass().isArray()) {
				builder = builder.array(entry.getKey(), entry.getValue());
			}
			else {
				builder = builder.field(entry.getKey(), entry.getValue());
			}
		}
		
		return builder;
	}
	
	public static void main(String arg[]) throws IOException{
		Map<String ,Object> map = new HashMap<String, Object>();
		String arg1[] = {"222","333"};
		map.put("1111", arg1);
		map.put("1112", arg1);
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder = builder.startObject();
		builder = addFields(map, builder);
		builder = builder.endObject();
		System.out.println(builder.string());
	}
}
