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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RankSorting {

	public static Job createJob() throws IOException {

		Job job = Job.getInstance(new Configuration(), "RankSorting"); //new Job(new Configuration(), "RankSorting");
		job.setJarByClass(RankSorting.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(RankSortingMapper.class);
		job.setReducerClass(RankSortingReducer.class);

		job.setSortComparatorClass(RankSortingReduceKeyComparator.class);
		job.setGroupingComparatorClass(RankSortingGroupingComparator.class);
		job.setPartitionerClass(RankSortingPartitioner.class);

		return job;
	}

	public static class RankSortingMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] object = value.toString().split("\t");
			StringBuilder sb = new StringBuilder();
			for (int i = 1; i < object.length; i++) {
				if (object[i] != null && object[i] != "") {
					Text propertyName = new Text(sb.append(i).append(":").append(object[i]).toString());
					sb.setLength(0);
					Text objectRank = new Text(sb.append(object[0]).append(":").append(object[i]).toString());
					sb.setLength(0);
					context.write(propertyName, objectRank);
				}
			}
		}

	}

	public static class RankSortingReducer extends Reducer<Text, Text, LongWritable, Text> {
		@Override
		protected void reduce(Text key, java.lang.Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int i = 1;
			for (Text t : values){
				context.write(new LongWritable(i),new Text(key.toString().substring(0, key.toString().indexOf(":") + 1) + t));//new Text(key.toString().substring(0, key.find(":"))), t);
				i++;
			}
		}
	}

	/**
	 * @author iabsalyamov This comparator implement sorting of the values in
	 *         reducer's iterator according to object's rank
	 */
	public static final class RankSortingReduceKeyComparator extends WritableComparator {
		protected RankSortingReduceKeyComparator() {
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

	public static final class RankSortingGroupingComparator extends WritableComparator {

		protected RankSortingGroupingComparator() {
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

	private static final class RankSortingPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String name = key.toString().substring(0, key.toString().indexOf(":"));
			return name.hashCode() % numPartitions;
		}

	}
}
