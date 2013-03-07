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

	protected static long topk;
	
	public static Job createJob(long topkNum) throws IOException {
		topk = topkNum;

		Job job = Job.getInstance(new Configuration(), "EndSorting"); 
		job.setJarByClass(EndSorting.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(EndSortingMapper.class);
		job.setReducerClass(EndSortingReducer.class);

		job.setSortComparatorClass(EndSortingReduceKeyComparator.class);
		job.setGroupingComparatorClass(EndSortingGroupingComparator.class);
		job.setPartitionerClass(EndSortingPartitioner.class);

		return job;
	}

	public static class EndSortingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// input:  oid  first:last
		// output: topk:last  first:last
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//String[] item = value.toString().split(":");
			context.write(new Text(value.toString().substring(value.toString().indexOf(":")+1)), value);
		}
	}

	public static class EndSortingReducer extends Reducer<Text, Text, LongWritable, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//String[] object = value.toString().split(":");
			long i = 1;
			long lastcheck = topk;
			long lastline = 0;
			for(Text t : values){
				if(i == topk){
					lastcheck = Long.valueOf(t.toString().substring(t.toString().indexOf(":")+1).toString());
				}
				long firstcheck = Long.valueOf(t.toString().substring(0, t.toString().indexOf(":")));
				if(firstcheck <= lastcheck){
					context.write(new LongWritable(i++), new Text(t));
					lastline = Long.valueOf(t.toString().substring(t.toString().indexOf(":")+1).toString());
					context.getCounter(TopkCounter.numOfObjects).setValue(lastline);
					// lastline is the one we want!!!!!
				}
				i++;
			}
			context.write(new LongWritable(lastline), new Text("I am the last line !!!!!"));
		}
	}

	public static final class EndSortingReduceKeyComparator extends WritableComparator {
		protected EndSortingReduceKeyComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
				return Integer.valueOf(a.toString()).compareTo(Integer.valueOf(b.toString()));
		}
	}

	public static final class EndSortingGroupingComparator extends WritableComparator {

		protected EndSortingGroupingComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return 0;
		}
	}

	private static final class EndSortingPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return 0;
		}

	}
}
