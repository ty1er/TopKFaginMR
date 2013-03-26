package edu.ucr.cs236;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import edu.ucr.cs236.RankSorting.RankSortingGroupingComparator;
import edu.ucr.cs236.RankSorting.RankSortingMapper;
import edu.ucr.cs236.RankSorting.RankSortingReduceKeyComparator;
import edu.ucr.cs236.RankSorting.RankSortingReducer;

public class RankSortingTest {

	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, LongWritable, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, LongWritable, Text> mapReduceDriver;

	@Before
	public void setUp() {
		RankSortingMapper mapper = new RankSortingMapper();
		RankSortingReducer reducer = new RankSortingReducer();
		RankSortingGroupingComparator groupingComparator = new RankSortingGroupingComparator();
		RankSortingReduceKeyComparator keyComparator = new RankSortingReduceKeyComparator();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<Text, Text, LongWritable, Text>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, LongWritable, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		mapReduceDriver.setKeyGroupingComparator(groupingComparator);
		mapReduceDriver.setKeyOrderComparator(keyComparator);
	}

	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(), new Text("1	0.9	0.8	0.7"));
		mapDriver.withOutput(new Text("1:0.9"), new Text("1:0.9"));
		mapDriver.withOutput(new Text("2:0.8"), new Text("1:0.8"));
		mapDriver.withOutput(new Text("3:0.7"), new Text("1:0.7"));
		mapDriver.runTest();
	}

//	@Test
	public void testReducer() {
		List<Text> values1 = new ArrayList<Text>();
		List<Text> values2 = new ArrayList<Text>();
		values2.add(new Text("o2:1"));
		values1.add(new Text("o1:0.8"));
		reduceDriver.withInput(new Text("p1:0.8"), values1);
		reduceDriver.withInput(new Text("p1:1"), values2);
		reduceDriver.withOutput(new LongWritable(1), new Text("o2:1,o1:0.8"));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(), new Text("1	0.9	0.8	0.7"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2	1	0.8	0.95"));
		mapReduceDriver.withOutput(new LongWritable(1), new Text("1:2:1"));
		mapReduceDriver.withOutput(new LongWritable(2), new Text("1:1:0.9"));
		mapReduceDriver.withOutput(new LongWritable(1), new Text("2:1:0.8"));
		mapReduceDriver.withOutput(new LongWritable(2), new Text("2:2:0.8"));
		mapReduceDriver.withOutput(new LongWritable(1), new Text("3:2:0.95"));
		mapReduceDriver.withOutput(new LongWritable(2), new Text("3:1:0.7"));
		mapReduceDriver.runTest();
	}
}