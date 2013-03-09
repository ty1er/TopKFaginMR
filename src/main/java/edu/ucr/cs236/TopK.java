package edu.ucr.cs236;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
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

public class TopK {

public static long topk;
	
	public static Job createJob(long topkNum) throws IOException {
		topk = topkNum;

		Job job = Job.getInstance(new Configuration(), "TopK"); 
		job.setJarByClass(TopK.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);

		return job;
	}

	// input format:    lineNum     propId:objectIid:value
	// output format:   objectId     value
	public static class TopKMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String oid = value.toString().substring(value.toString().indexOf(":") + 1);
			if(Long.valueOf(key.toString()) <= topk)
				context.write(new Text(oid.toString().substring(0, oid.toString().indexOf(":"))), new Text(oid.toString().substring(oid.toString().indexOf(":")+1)));
		}
	}

	// input:  objectId     value
	// output: objectId		sum of values
	public static class TopKReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, java.lang.Iterable<Text> values, Context context) throws IOException, InterruptedException {
			float attr = (float) 0.0;
			for(Text value : values) {
				attr += Float.valueOf(value.toString());
			}
			context.write(key, new Text(Float.toString(attr)));
		}
	}
	
}
