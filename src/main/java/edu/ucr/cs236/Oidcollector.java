package edu.ucr.cs236;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Oidcollector {

	public static Job createJob() throws IOException {

		Job job = Job.getInstance(new Configuration(), "Oidcollector"); //new Job(new Configuration(), "Oidcollector");
		job.setJarByClass(Oidcollector.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(OidcollectorMapper.class);
		job.setReducerClass(OidcollectorReducer.class);

		return job;
	}

	public static class OidcollectorMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] object = value.toString().split("\t");
			String[] item = object[1].toString().split(":");
			StringBuilder sb = new StringBuilder();
			Text propertyName = new Text(sb.append(item[1]).append(":").append(object[0]).toString());
			sb.setLength(0);
			Text objectRank = new Text(sb.append(object[0]).toString());
			sb.setLength(0);
			context.write(propertyName, objectRank);
		}
	}

	public static class OidcollectorReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, java.lang.Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for (Text t : values)
				sb.append(t).append(":");
			context.write(new Text(key.toString()),new Text(sb.toString()));
		}
	}
}
 