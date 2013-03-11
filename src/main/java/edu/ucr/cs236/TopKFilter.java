package edu.ucr.cs236;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TopKFilter {

	public static long topK;

	public static Job createJob(long topKNum) throws IOException {
		topK = topKNum;

		Job job = Job.getInstance(new Configuration(), "TopKFilter");
		job.setJarByClass(TopKFilter.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(TopKFilterMapper.class);
		job.setReducerClass(TopKFilterReducer.class);
		job.setPartitionerClass(TopKFilterPartitioner.class);
		job.setSortComparatorClass(TopKFilterReduceKeyComparator.class);

		return job;
	}

	public static class TopKFilterMapper extends Mapper<LongWritable, FloatWritable, FloatWritable, LongWritable> {

		@Override
		protected void map(LongWritable key, FloatWritable value, Context context) throws IOException, InterruptedException {
			context.write(value, key);
		}
	}

	public static class TopKFilterReducer extends Reducer<FloatWritable, LongWritable, LongWritable, FloatWritable> {
		@Override
		protected void reduce(FloatWritable key, java.lang.Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			for (LongWritable value : values) {
				if (topK-- > 0)
				context.write(value, key);
			}
		}
	}
	
	public static final class TopKFilterReduceKeyComparator extends WritableComparator {
		protected TopKFilterReduceKeyComparator() {
			super(FloatWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
				return -1*Float.valueOf(a.toString()).compareTo(Float.valueOf(b.toString()));
		}
	}
	
	private static final class TopKFilterPartitioner extends Partitioner<FloatWritable, LongWritable> {

		@Override
		public int getPartition(FloatWritable key, LongWritable value, int numPartitions) {
			return 0;
		}

	}

}
