package cn.thousfeet.hadoop.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
			
		long upFlowCount = 0;
		long downFlowCount = 0;
		
		for(FlowBean bean : values)
		{
			upFlowCount += bean.getupFlow();
			downFlowCount += bean.getDownFlow();
		}
		
		context.write(key, new FlowBean(key.toString(),upFlowCount,downFlowCount));
		
	}
}
