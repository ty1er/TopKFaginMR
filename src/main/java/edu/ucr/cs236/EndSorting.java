package edu.ucr.cs236;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class EndSorting {

	public static Job createJob() throws IOException {
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

		// input:  objectId  firstOccurence:lastOccurence
		// output: lastOccurence  firstOccurence:lastOccurence
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text(value.toString().substring(value.toString().indexOf(":")+1)), value);
		}
	}

	// input: lastOccurence  firstOccurence:lastOccurence
	// output: -
	public static class EndSortingReducer extends Reducer<Text, Text, LongWritable, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			long i = 1;
			long lastcheck = Long.MAX_VALUE;
			long lastline = 0;
			for(Text t : values){
				int topk = context.getConfiguration().getInt("topK", -1);
				if(i == topk){
					lastcheck = Long.valueOf(t.toString().substring(t.toString().indexOf(":")+1).toString());
				}
				long firstcheck = Long.valueOf(t.toString().substring(0, t.toString().indexOf(":")));
				if(firstcheck <= lastcheck){
					lastline = Long.valueOf(t.toString().substring(t.toString().indexOf(":")+1).toString());
					context.write(new LongWritable(i), t);
					context.getCounter(TopkCounter.maxLineNumber).setValue(lastline);
				}
				i++;
			}
			context.write(new LongWritable(lastline), null);
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
