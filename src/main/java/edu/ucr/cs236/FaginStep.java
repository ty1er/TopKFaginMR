package edu.ucr.cs236;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FaginStep {

	public static Job createJob() throws IOException {
		Job job = Job.getInstance(new Configuration(), "FaginStep");
		job.setJarByClass(RankSorting.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(FaginStepMapper.class);
		job.setReducerClass(FaginStepReducer.class);

		return job;
	}
	

	public static class FaginStepMapper extends Mapper<LongWritable, Text, Text, Text> {
	}

	public static class FaginStepReducer extends Reducer<Text, Text, Text, Text> {
	}

}
