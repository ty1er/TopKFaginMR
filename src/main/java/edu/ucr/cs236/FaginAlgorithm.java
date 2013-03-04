package edu.ucr.cs236;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
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
		FileSystem hdfs = FileSystem.get(sortingJob.getConfiguration());
		Path outputPath1 = new Path(args[1] + "/sorting");
		if (hdfs.exists(outputPath1))
			hdfs.delete(outputPath1, true);
		FileInputFormat.addInputPath(sortingJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(sortingJob, outputPath1);
		sortingJob.setNumReduceTasks(Integer.parseInt(args[2]));

		Job LinesJob = LineSorting.createJob();
		Path outputPath2 = new Path(args[1] + "/lines");
		if (hdfs.exists(outputPath2))
			hdfs.delete(outputPath2, true);
		FileInputFormat.addInputPath(LinesJob, outputPath1);
		FileOutputFormat.setOutputPath(LinesJob, outputPath2);
		
		Job EndJob = EndSorting.createJob();
		Path outputPath3 = new Path(args[1] + "/end");
		if (hdfs.exists(outputPath3))
			hdfs.delete(outputPath3, true);
		FileInputFormat.addInputPath(EndJob, outputPath2);
		FileOutputFormat.setOutputPath(EndJob, outputPath3);

		ControlledJob controlledSortingJob = new ControlledJob(sortingJob.getConfiguration());
		ControlledJob controlledLinesJob = new ControlledJob(LinesJob.getConfiguration());
		ControlledJob controlledEndJob = new ControlledJob(EndJob.getConfiguration());
		
		controlledLinesJob.addDependingJob(controlledSortingJob);
		controlledEndJob.addDependingJob(controlledLinesJob);

		JobControl jc = new JobControl("FaginAlgorithm");
		jc.addJob(controlledSortingJob);
		jc.addJob(controlledLinesJob);
		jc.addJob(controlledEndJob);

		Thread runjobc = new Thread(jc);
		runjobc.start();

		
		while (!jc.allFinished()) {
			System.out.println("Jobs in waiting state: " + jc.getWaitingJobList().size());
			//System.out.println("Jobs in ready state: " + jc.getReadyJobsList().size());
			System.out.println("Jobs in running state: " + jc.getRunningJobList().size());
			System.out.println("Jobs in success state: " + jc.getSuccessfulJobList().size());
			System.out.println("Jobs in failed state: " + jc.getFailedJobList().size());
			System.out.println("\n");
			try {
				Thread.sleep(5000);
			} catch (Exception e) {
			}
		}
		
		jc.stop();
		
		return 0;
	}
}
