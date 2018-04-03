package cn.thousfeet.hadoop.mapreduce.areapartition;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Partitioner;

public class AreaPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE>{

	private static HashMap<String, Integer> areaMap = new HashMap<>();
	
	static {
		
		areaMap.put("135", 0);
		areaMap.put("136", 1);
		areaMap.put("137", 2);
		areaMap.put("138", 3);
		areaMap.put("139", 4);
		
//		loadTableToAreaMap(areaMap);
	}
	
	@Override
	public int getPartition(KEY key, VALUE value, int numPartiotions) {

		//从Key中获取手机号，查询归属地字典，不同省份返回不同组号
		return areaMap.get(key.toString().substring(0,3)) == null ? 5 : areaMap.get(key.toString().substring(0,3));
		
	}

	
	
}
