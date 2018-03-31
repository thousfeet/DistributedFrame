package cn.thousfeet.hadoop.mapreduce.flowsum;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {

		//读入一行，切分数据
		String line = value.toString();
		String[] fields = StringUtils.split(line, "\t");
		
		String phoneNum = fields[1];
		long upFlow = Long.parseLong(fields[7]);
		long downFlow = Long.parseLong(fields[8]);
		
		//封装数据为key-value并输出
		context.write(new Text(phoneNum), new FlowBean(phoneNum,upFlow,downFlow));
		
	}
}
