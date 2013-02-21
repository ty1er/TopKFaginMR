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
//		setConf(sortingJob.getConfiguration());
		
		FileSystem hdfs = FileSystem.get(sortingJob.getConfiguration());
		Path outputPath1 = new Path(args[1] + "/sorting");
		if (hdfs.exists(outputPath1))
			hdfs.delete(outputPath1, true);
		FileInputFormat.addInputPath(sortingJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(sortingJob, outputPath1);
		sortingJob.setNumReduceTasks(Integer.parseInt(args[2]));
		
		Job preprocessJob = FaginPreprocess.createJob();
		Path outputPath2 = new Path(args[1] + "/preprocess");
		if (hdfs.exists(outputPath2))
			hdfs.delete(outputPath2, true);
		FileInputFormat.addInputPath(preprocessJob, outputPath1);
		FileOutputFormat.setOutputPath(preprocessJob, outputPath2);
		
		Job faginStepJob = FaginStep.createJob();
		Path outputPath3 = new Path(args[1] + "/faginStep");
		if (hdfs.exists(outputPath3))
			hdfs.delete(outputPath3, true);
		FileInputFormat.addInputPath(faginStepJob, outputPath2);
		FileOutputFormat.setOutputPath(faginStepJob, outputPath3);
//		preprocessStepJob.setNumReduceTasks(Integer.parseInt(args[2]));
		
		ControlledJob controlledSortingJob = new ControlledJob(sortingJob.getConfiguration());
		ControlledJob controlledPreprocessJob = new ControlledJob(preprocessJob.getConfiguration());
		ControlledJob controlledFaginStepJob = new ControlledJob(faginStepJob.getConfiguration());

		controlledPreprocessJob.addDependingJob(controlledSortingJob);
		controlledFaginStepJob.addDependingJob(controlledPreprocessJob);

		JobControl jc = new JobControl("FaginAlgorithm");
		jc.addJob(controlledSortingJob);
		jc.addJob(controlledPreprocessJob);
		jc.addJob(controlledFaginStepJob);

		Thread runjobc = new Thread(jc);
		runjobc.start();

		while (!jc.allFinished()) {
			System.out.println("Jobs in waiting state: " + jc.getWaitingJobList().size());
			System.out.println("Jobs in ready state: " + jc.getReadyJobsList().size());
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
