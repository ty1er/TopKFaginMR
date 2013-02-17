package edu.ucr.cs236;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
		setConf(sortingJob.getConfiguration());
		FileInputFormat.addInputPath(sortingJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(sortingJob, new Path("/tmp/fagin"));
		sortingJob.setNumReduceTasks(Integer.parseInt(args[2]));
		
		Job algorithmStepJob = FaginStep.createJob();
		FileInputFormat.addInputPath(algorithmStepJob, new Path("/tmp/fagin"));
		FileOutputFormat.setOutputPath(algorithmStepJob, new Path(args[1]));
		algorithmStepJob.setNumReduceTasks(Integer.parseInt(args[2]));
		
		ControlledJob controlledSortingJob = new ControlledJob(sortingJob.getConfiguration());
		ControlledJob controlledAlgorithmStepJob = new ControlledJob(algorithmStepJob.getConfiguration());

		controlledAlgorithmStepJob.addDependingJob(controlledSortingJob);

		JobControl jc = new JobControl("FaginAlgorithm");
		jc.addJob(controlledSortingJob);
		jc.addJob(controlledAlgorithmStepJob);

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
