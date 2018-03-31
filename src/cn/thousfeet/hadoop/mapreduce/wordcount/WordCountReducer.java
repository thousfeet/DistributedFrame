package cn.thousfeet.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> valueList,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		
		long count = 0;
		
		for(LongWritable value : valueList) //遍历value list累加求和
		{
			count += value.get();
		}
		
		context.write(key, new LongWritable(count)); //输出这一个单词的统计结果
	}
}
