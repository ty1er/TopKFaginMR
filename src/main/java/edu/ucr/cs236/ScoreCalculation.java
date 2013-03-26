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

public class ScoreCalculation {

	public static Job createJob() throws IOException {
		Job job = Job.getInstance(new Configuration(), "ScoreCalculation");
		job.setJarByClass(ScoreCalculation.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(ScoreCalculationMapper.class);
		job.setReducerClass(ScoreCalculationReducer.class);

		return job;
	}

	// input format: lineNum propId:objectIid:value
	// output format: objectId value
	public static class ScoreCalculationMapper extends Mapper<LongWritable, Text, LongWritable, FloatWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String oid = value.toString().substring(value.toString().indexOf(":") + 1);
			int maxLine = context.getConfiguration().getInt("maxLineNum", -1);
			if (key.get() <= maxLine)
				context.write(new LongWritable(Long.valueOf(oid.toString().substring(0, oid.toString().indexOf(":")))),
						new FloatWritable(Float.valueOf(oid.toString().substring(oid.toString().indexOf(":") + 1))));
		}
	}

	// input: objectId value
	// output: objectId sum of values
	public static class ScoreCalculationReducer extends Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {
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
