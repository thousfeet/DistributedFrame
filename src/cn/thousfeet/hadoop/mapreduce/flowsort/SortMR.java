package cn.thousfeet.hadoop.mapreduce.flowsort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.thousfeet.hadoop.mapreduce.flowsum.FlowBean;

/**
 * 按流量排序的流量统计的MR程序
 * map和reduce作为静态内部类写在一个类中
 * @author thousfeet
 *
 */
public class SortMR {
	
	public static class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] fields = StringUtils.split(line,"\t");
			
			String phoneNum = fields[0];
			long upFlow = Long.parseLong(fields[1]);
			long downFlow = Long.parseLong(fields[2]);
			
			context.write(new FlowBean(phoneNum,upFlow,downFlow), NullWritable.get());
		}
	}
	
	
	public static class SortReducer extends Reducer<FlowBean, NullWritable, Text, FlowBean>{
		
		@Override
		protected void reduce(FlowBean key, Iterable<NullWritable> values,
				Reducer<FlowBean, NullWritable, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {

			String phoneNum = key.getPhoneNum();
			context.write(new Text(phoneNum), key);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SortMR.class);
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
