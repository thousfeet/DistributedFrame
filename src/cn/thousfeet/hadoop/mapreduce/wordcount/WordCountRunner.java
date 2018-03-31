package cn.thousfeet.hadoop.mapreduce.wordcount;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WordCountRunner {

		public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			conf.set("mapreduce.job.jar","wordcount.jar");
			
			Job job = Job.getInstance(conf);
			
			//设置整个job所用的那些类在哪个jar包
			job.setJarByClass(WordCountRunner.class);
			
			//指定job使用的mapper和reducer类
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			
			//指定reduce和mapper的输出数据key-value类型
			job.setOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
			
			//指定mapper的输出数据key-value类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
			
			//指定原始输入数据的存放路径
			FileInputFormat.setInputPaths(job, new Path("hdfs://node01:9000/wordcount/srcdata/"));
			
			//指定处理结果数据的存放路径
			FileOutputFormat.setOutputPath(job, new Path("hdfs://node01:9000/wordcount/output/"));
		
			//将job提交给集群运行 参数为true时会打印运行进度
			job.waitForCompletion(true);
		}
}
