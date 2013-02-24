package edu.ucr.cs236;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class FaginStep {

	protected static int properties;
	protected static int iteration;

	public static Job createJob(int iterationNum, int propertiesNum) throws IOException {
		properties = propertiesNum;
		iteration = iterationNum;

		Job job = Job.getInstance(new Configuration(), "FaginStep");
		job.setJarByClass(RankSorting.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(FaginStepMapper.class);
		job.setReducerClass(FaginStepReducer.class);

		return job;
	}

	static class FaginStepMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// this line is currently active line of the source file
			if (key.get() == iteration) {
				String[] objects = value.toString().split(";");
				for (String object : objects) {
					String objName = object.substring(0, object.indexOf(":"));
					context.write(new LongWritable(0), new Text(objName + ":1"));
				}
				context.write(key, value);
			}
			// this line is extra line for storing objects seen list
			else if (key.get() == 0) {
				String[] objects = value.toString().split(";");
				for (String object : objects)
					context.write(key, new Text(object));
			}
			else
				context.write(key, value);
		}
	}

	static class FaginStepReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.get() == 0) {
				Map<String, Long> obejctsSeenMap = new HashMap<String, Long>();
				for (Text value : values) {
					if (!value.toString().equals("")) {
						String objName = value.toString().substring(0, value.toString().indexOf(":"));
						Long objCount = Long.valueOf(value.toString().substring(value.toString().indexOf(":") + 1));
						if (obejctsSeenMap.containsKey(objName))
							obejctsSeenMap.put(objName, obejctsSeenMap.get(objName) + objCount);
						else
							obejctsSeenMap.put(objName, objCount);
					}
				}
				int topKObjects = 0;
				StringBuffer sb = new StringBuffer();
				for (Entry<String, Long> obj : obejctsSeenMap.entrySet()) {
					sb.append(obj.getKey()).append(":").append(obj.getValue()).append(";");
					if (obj.getValue() == properties)
						topKObjects++;
				}
				sb.deleteCharAt(sb.length() - 1);
				context.write(new LongWritable(0), new Text(sb.toString()));
				context.getCounter(FaginStepTopkObjectCounter.numOfObjects).setValue(topKObjects);
			} else
				context.write(key, values.iterator().next());
		}
	}

}
