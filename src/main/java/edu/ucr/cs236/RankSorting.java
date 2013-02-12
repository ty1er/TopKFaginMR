package edu.ucr.cs236;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RankSorting {

	public Job createJob() throws IOException {

		Job job = new Job(new Configuration(), "FaginAlgorithm");
		job.setJarByClass(getClass());

		// job.getConfiguration().set("textinputformat.record.delimiter", "\n");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(RankSortingMapper.class);
		job.setReducerClass(RankSortingReducer.class);

		job.setSortComparatorClass(RankSortingKeyComparator.class);
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
					Text propertyName = new Text(sb.append("p").append(i).append(":").append(object[i]).toString());
					sb.setLength(0);
					Text objectRank = new Text(sb.append("o").append(object[0]).append(":").append(object[i]).toString());
					sb.setLength(0);
					context.write(propertyName, objectRank);
				}
			}
		}

	}

	public static class RankSortingReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, java.lang.Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text t : values)
				sb.append(t).append(",");
			sb.setLength(sb.length() - 1);
			context.write(new Text(key.toString().substring(0, key.find(":"))), new Text(sb.toString()));
		}
	}

	/**
	 * @author iabsalyamov This comparator implement sorting of the values in
	 *         reducer's iterator according to object's rank
	 */
	public static final class RankSortingKeyComparator implements RawComparator<Text> {

		@Override
		public int compare(Text o1, Text o2) {
			String o1Name = o1.toString().substring(0, o1.toString().indexOf(":"));
			String o2Name = o2.toString().substring(0, o2.toString().indexOf(":"));

			int nameCompare = o1Name.compareTo(o2Name);
			if (nameCompare == 0) {
				Float o1Rank = Float.parseFloat(o1.toString().substring(o1.toString().indexOf(":") + 1));
				Float o2Rank = Float.parseFloat(o2.toString().substring(o2.toString().indexOf(":") + 1));
				return -1 * o1Rank.compareTo(o2Rank);
			}
			return nameCompare;
		}

		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
			return 0;
		}

	}

	public static final class RankSortingGroupingComparator implements RawComparator<Text> {

		@Override
		public int compare(Text o1, Text o2) {
			String o1Name = o1.toString().substring(0, o1.toString().indexOf(":"));
			String o2Name = o2.toString().substring(0, o2.toString().indexOf(":"));
			return o1Name.compareTo(o2Name);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return 0;
		}
	}

	private static final class RankSortingPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String name = key.toString().substring(0, key.toString().indexOf(":") - 1);
			return name.hashCode() % numPartitions;
		}

	}
}
