package edu.ucr.cs236;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LineSorting {

	public static Job createJob() throws IOException {

		Job job = Job.getInstance(new Configuration(), "LineSorting"); 
		job.setJarByClass(LineSorting.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(LineSortingMapper.class);
		job.setReducerClass(LineSortingReducer.class);

		return job;
	}

	// input format:    line     pid:oid:val
	// output format:   oid:line     line
	public static class LineSortingMapper extends Mapper<IntWritable, Text, Text, IntWritable> {

		@Override
		protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
			String oidk = value.toString().substring(value.toString().indexOf(":") + 1);
			context.write(new Text(oidk.toString().substring(0, oidk.toString().indexOf(":"))), key); 
			// key = oid:line, value = line
		}
	}

	
	public static class LineSortingReducer extends Reducer<Text, IntWritable, Text, Text> {
		@Override
		// input:  oid:line   line
		// output: oid:line1, line2, line3, ... (choose max and min)
		protected void reduce(Text key, java.lang.Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			int max = Integer.MIN_VALUE;
			int min = Integer.MAX_VALUE;
			for(IntWritable value : values) {
				if (value.get() > max)
					max = value.get();
				if (value.get() < min)
					min = value.get();
			}
			context.write(key, new Text(min + ":" + max));
		}
	}
	
}
