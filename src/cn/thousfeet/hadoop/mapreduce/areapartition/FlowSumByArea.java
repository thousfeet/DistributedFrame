package cn.thousfeet.hadoop.mapreduce.areapartition;

import java.io.IOException;

import javax.swing.JProgressBar;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cn.thousfeet.hadoop.mapreduce.flowsum.FlowBean;

public class FlowSumByArea {

	public static class FlowSumByAreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] fields = StringUtils.split(line, "\t");
			
			String phoneNum = fields[1];
			long upFlow = Long.parseLong(fields[7]);
			long downFlow = Long.parseLong(fields[8]);
			
			context.write(new Text(phoneNum), new FlowBean(phoneNum,upFlow,downFlow));
		}
		
	}
	
	public static class FlowSumByAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
		
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
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new org.apache.hadoop.conf.Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowSumByArea.class);
		
		job.setMapperClass(FlowSumByAreaMapper.class);
		job.setReducerClass(FlowSumByAreaReducer.class);
		
		//设置自定义的分组逻辑
		job.setPartitionerClass(AreaPartitioner.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//设置reducer任务数（与partition中的分组数一致）
		job.setNumReduceTasks(6);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
