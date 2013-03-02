package edu.ucr.cs236;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LineSorting {

	public static Job createJob() throws IOException {

		Job job = Job.getInstance(new Configuration(), "LineSorting"); 
		job.setJarByClass(LineSorting.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(LineSortingMapper.class);
		job.setReducerClass(LineSortingReducer.class);

		job.setSortComparatorClass(LineSortingReduceKeyComparator.class);
		job.setGroupingComparatorClass(LineSortingGroupingComparator.class);
		job.setPartitionerClass(LineSortingPartitioner.class);

		return job;
	}

	// input format:    line     pid:oid:val
	// output format:   oid:line     line
	public static class LineSortingMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String oidk = value.toString().substring(value.toString().indexOf(":") + 1);
			context.write(new Text(oidk.toString().substring(0, oidk.toString().indexOf(":")) + ":" + key), new Text(key)); 
			// key = oid:line, value = line
		}
	}

	
	public static class LineSortingReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, java.lang.Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			Iterator<Text> iter = values.iterator();
			Text last = iter.next();
			if (last != null)
				sb.append(last);
			do {
				last = iter.next();
			} while(last != null);
			context.write(new Text(key.toString().substring(0, key.toString().indexOf(":"))), new Text(sb.toString()));

		}
	}
	
	
	public final class LineSortingReduceKeyComparator extends WritableComparator {
		protected LineSortingReduceKeyComparator() {
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
				return -1 * Float.valueOf(o1Items[1]).compareTo(Float.valueOf(o2Items[1]));
			}
			return nameCompare;
		}

	}

	public final class LineSortingGroupingComparator extends WritableComparator {

		protected LineSortingGroupingComparator() {
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

	final class LineSortingPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String name = key.toString().substring(0, key.toString().indexOf(":"));
			return name.hashCode() % numPartitions;
		}
	}
	
}
