package edu.ucr.cs236;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class FaginPreprocess {
	public static Job createJob() throws IOException {
		Job job = Job.getInstance(new Configuration(), "FaginPreprocessingStep");
		job.setJarByClass(RankSorting.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(FaginPreprocessMapper.class);
		job.setReducerClass(FaginPreprocessReducer.class);
		
		job.setGroupingComparatorClass(FaginPreprocessGroupingComparator.class);
		job.setSortComparatorClass(FaginPreprocessSortComparator.class);
		job.setPartitionerClass(FaginPreprocessPartitioner.class);

		return job;
	}

	public static class FaginPreprocessMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String propertyName = value.toString().substring(0, value.toString().indexOf(":"));
			context.write(new Text(key + ":" + propertyName), new Text(value.toString().substring(value.toString().indexOf(":") + 1)));
		}
	}

	public static class FaginPreprocessReducer extends Reducer<Text, Text, LongWritable, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for (Text value : values)
			{
				String[] item = value.toString().split(":");
				sb.append(item[0].toString()).append(":").append(item[2].toString()).append(";");
			}
			sb.deleteCharAt(sb.length() - 1);
			Long lineNum = Long.valueOf(key.toString().substring(0, key.toString().indexOf(":")));
			context.write(new LongWritable(lineNum), new Text(sb.toString()));
			//producing extra line with lineNum 0 to store objects seen so far
			if (lineNum.equals(1))
				context.write(new LongWritable(0), new Text());
		}
	}

	public static class FaginPreprocessGroupingComparator extends WritableComparator {

		protected FaginPreprocessGroupingComparator() {
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

	public static class FaginPreprocessSortComparator extends WritableComparator {

		protected FaginPreprocessSortComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text t1 = (Text) a;
			Text t2 = (Text) b;

			String[] o1Items = t1.toString().split(":");
			String[] o2Items = t2.toString().split(":");

			int nameCompare = Integer.valueOf(o1Items[0]).compareTo(Integer.valueOf(o2Items[0]));
			if (nameCompare == 0) {
				return -1 * o1Items[1].compareTo(o2Items[1]);
			}
			return nameCompare;
		}
	}

	public static class FaginPreprocessPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String lineNum = key.toString().substring(0, key.toString().indexOf(":"));
			return lineNum.hashCode() % numPartitions;
		}
	}
}
