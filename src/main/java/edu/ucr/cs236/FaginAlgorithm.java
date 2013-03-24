package edu.ucr.cs236;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FaginAlgorithm extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new FaginAlgorithm(), args);
		System.exit(res);
	}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Job sortingJob = RankSorting.createJob();
		if (args.length > 3) {
			sortingJob.getConfiguration().setLong("normalization", Integer.valueOf(args[3]));
		}
		FileSystem hdfs = FileSystem.get(sortingJob.getConfiguration());
		Path outputPath1 = new Path(args[1] + "/sorting");
		if (hdfs.exists(outputPath1))
			hdfs.delete(outputPath1, true);
		FileInputFormat.addInputPath(sortingJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(sortingJob, outputPath1);
//		sortingJob.setNumReduceTasks(Integer.parseInt(args[2]));
		boolean sortingJobCompletion = sortingJob.waitForCompletion(true);
		if (!sortingJobCompletion)
			return 0;

		Job linesJob = ObjectOccurrence.createJob();
		Path outputPath2 = new Path(args[1] + "/occurrence");
		if (hdfs.exists(outputPath2))
			hdfs.delete(outputPath2, true);
		FileInputFormat.addInputPath(linesJob, outputPath1);
		FileOutputFormat.setOutputPath(linesJob, outputPath2);
		boolean lineJobCompletion =linesJob.waitForCompletion(true);
		if (!lineJobCompletion)
			return 0;
		
		Job endJob = EndSorting.createJob();
		endJob.getConfiguration().setLong("topK", Integer.valueOf(args[2]));
		Path outputPath3 = new Path(args[1] + "/end");
		if (hdfs.exists(outputPath3))
			hdfs.delete(outputPath3, true);
		FileInputFormat.addInputPath(endJob, outputPath2);
		FileOutputFormat.setOutputPath(endJob, outputPath3);
		boolean endJobCompletion = endJob.waitForCompletion(true);
		if (!endJobCompletion)
			return 0;

 		Job topKJob = ScoreCalculation.createJob();
 		topKJob.getConfiguration().setLong("maxLineNum", endJob.getCounters().findCounter(TopkCounter.maxLineNumber).getValue());
 		Path outputPath4 = new Path(args[1] + "/score");
		FileInputFormat.addInputPath(topKJob, outputPath1);
		if (hdfs.exists(outputPath4))
			hdfs.delete(outputPath4, true);
		FileOutputFormat.setOutputPath(topKJob, outputPath4);
		boolean topKJobCompletion = topKJob.waitForCompletion(true);
		if (!topKJobCompletion)
			return 0;
		
		Job topKFilterJob = TopKFilter.createJob();
		topKFilterJob.getConfiguration().setLong("topK", Integer.valueOf(args[2]));
 		Path outputPath5 = new Path(args[1] + "/result");
		FileInputFormat.addInputPath(topKFilterJob, outputPath4);
		if (hdfs.exists(outputPath5))
			hdfs.delete(outputPath5, true);
		FileOutputFormat.setOutputPath(topKFilterJob, outputPath5);
		boolean topKFilterJobCompletion = topKFilterJob.waitForCompletion(true);
		if (!topKFilterJobCompletion)
			return 0;

		return 1;
	}
}
