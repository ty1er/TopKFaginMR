package edu.ucr.cs236;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LineSorting {

	public static Job createJob() throws IOException {

		Job job = Job.getInstance(new Configuration(), "LineSorting"); 
		job.setJarByClass(LineSorting.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(LineSortingMapper.class);
		job.setReducerClass(LineSortingReducer.class);

		return job;
	}

	// input format:    line     pid:oid:val
	// output format:   oid     line
	public static class LineSortingMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String oidk = value.toString().substring(value.toString().indexOf(":") + 1);
			context.write(new Text(oidk.toString().substring(0, oidk.toString().indexOf(":"))), key); 
		}
	}

	
	public static class LineSortingReducer extends Reducer<Text, LongWritable, Text, Text> {
		@Override
		// input:  oid   line
		// output: (max : min)
		protected void reduce(Text key, java.lang.Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long max = Long.MIN_VALUE;
			long min = Long.MAX_VALUE;
			for(LongWritable value : values) {
				if (value.get() > max)
					max = value.get();
				if (value.get() < min)
					min = value.get();
			}
			context.write(null, new Text(min + ":" + max));
		}
	}
	
}
