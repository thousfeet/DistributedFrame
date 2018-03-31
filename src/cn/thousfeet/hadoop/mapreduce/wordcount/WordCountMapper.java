package cn.thousfeet.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		String[] words = StringUtils.split(line," "); //切分单词
		
		for(String word : words) //遍历 输出为key-value( <word,1> )
		{
			context.write(new Text(word), new LongWritable(1));
		}
	
	}
	
}
