package edu.ucr.cs236;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class TopK {

	public static long maxLine;

	public static Job createJob(long maxLineNum) throws IOException {
		maxLine = maxLineNum;

		Job job = Job.getInstance(new Configuration(), "TopK");
		job.setJarByClass(TopK.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);

		return job;
	}

	// input format: lineNum propId:objectIid:value
	// output format: objectId value
	public static class TopKMapper extends Mapper<LongWritable, Text, LongWritable, FloatWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String oid = value.toString().substring(value.toString().indexOf(":") + 1);
			if (key.get() <= maxLine)
				context.write(new LongWritable(Long.valueOf(oid.toString().substring(0, oid.toString().indexOf(":")))),
						new FloatWritable(Float.valueOf(oid.toString().substring(oid.toString().indexOf(":") + 1))));
		}
	}

	// input: objectId value
	// output: objectId sum of values
	public static class TopKReducer extends Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {
		@Override
		protected void reduce(LongWritable key, java.lang.Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			float attr = (float) 0.0;
			for (FloatWritable value : values) {
				attr += value.get();
			}
			context.write(key, new FloatWritable(attr));
		}
	}

}
