package edu.ucr.cs236;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class FaginStepTest {
	
	MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
	ReduceDriver<LongWritable, Text, LongWritable, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text> mapReduceDriver;
	
	@Before
	public void setUp() {
		FaginStep.FaginStepMapper mapper = new FaginStep.FaginStepMapper();
		FaginStep.FaginStepReducer reducer = new FaginStep.FaginStepReducer();
		mapDriver = new MapDriver<LongWritable, Text, LongWritable, Text>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<LongWritable, Text, LongWritable, Text>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testSimple() {
		FaginStep.iteration = 1;
		FaginStep.properties = 3;
		mapReduceDriver.withInput(new LongWritable(0), new Text(""));
		mapReduceDriver.withInput(new LongWritable(1), new Text("o2:0.95;o1:0.9;o2:1"));
		mapReduceDriver.withInput(new LongWritable(2), new Text("o1:0.85;o2:0.9;o1:0.9"));
		mapReduceDriver.withOutput(new LongWritable(0), new Text("o2:2;o1:1"));
		mapReduceDriver.withOutput(new LongWritable(1), new Text("o2:0.95;o1:0.9;o2:1"));
		mapReduceDriver.withOutput(new LongWritable(2), new Text("o1:0.85;o2:0.9;o1:0.9"));
		mapReduceDriver.withCounter(FaginStepTopkObjectCounter.numOfObjects, 0);
		mapReduceDriver.runTest();
	}
	
	@Test
	public void testTopkCounter() {
		FaginStep.iteration = 1;
		FaginStep.properties = 3;
		mapReduceDriver.withInput(new LongWritable(0), new Text("o2:1;o3:1"));
		mapReduceDriver.withInput(new LongWritable(1), new Text("o2:0.95;o1:0.9;o2:1"));
		mapReduceDriver.withInput(new LongWritable(2), new Text("o1:0.85;o2:0.9;o1:0.9"));
		mapReduceDriver.withOutput(new LongWritable(0), new Text("o2:3;o1:1;o3:1"));
		mapReduceDriver.withOutput(new LongWritable(1), new Text("o2:0.95;o1:0.9;o2:1"));
		mapReduceDriver.withOutput(new LongWritable(2), new Text("o1:0.85;o2:0.9;o1:0.9"));
		mapReduceDriver.withCounter(FaginStepTopkObjectCounter.numOfObjects, 1);
		mapReduceDriver.runTest();
	}


}
