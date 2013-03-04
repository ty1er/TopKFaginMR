package edu.ucr.cs236;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class EndSorting {

	protected static int topk;
	
	public static Job createJob(int topkNum) throws IOException {
		topk = topkNum;

		Job job = Job.getInstance(new Configuration(), "EndSorting"); 
		job.setJarByClass(EndSorting.class);

		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(EndSortingMapper.class);
		job.setReducerClass(EndSortingReducer.class);

		job.setSortComparatorClass(EndSortingReduceKeyComparator.class);
		job.setGroupingComparatorClass(EndSortingGroupingComparator.class);
		job.setPartitionerClass(EndSortingPartitioner.class);

		return job;
	}

	public static class EndSortingMapper extends Mapper<Text, Text, Text, Text> {

		// input:  oid  first:last
		// output: topk:last  first:last
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			//String[] item = value.toString().split(":");
			context.write(new Text(topk + ":" + value.toString().substring(value.toString().indexOf(":")+1)), value);
		}
	}

	public static class EndSortingReducer extends Reducer<Text, Text, IntWritable, Text> {
		protected void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
			//String[] object = value.toString().split(":");
			int i = 1;
			context.write(new IntWritable(i++),new Text(value));
		}
	}

	public static final class EndSortingReduceKeyComparator extends WritableComparator {
		protected EndSortingReduceKeyComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text t1 = (Text) a;
			Text t2 = (Text) b;

			String[] o1Items = t1.toString().split(":");
			String[] o2Items = t2.toString().split(":");

			int nameCompare = o1Items[0].compareTo(o2Items[0]);
			if (nameCompare == 0) {
				return Integer.valueOf(o1Items[1]).compareTo(Integer.valueOf(o2Items[1]));
			}
			return nameCompare;
		}

	}

	public static final class EndSortingGroupingComparator extends WritableComparator {

		protected EndSortingGroupingComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text t1 = (Text) a;
			Text t2 = (Text) b;
			String[] o1Items = t1.toString().split(":");
			String[] o2Items = t2.toString().split(":");

			return o1Items[0].compareTo(o2Items[0]);
		}
	}

	private static final class EndSortingPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String name = key.toString().substring(0, key.toString().indexOf(":"));
			return name.hashCode();// % numPartitions;
		}

	}
}
