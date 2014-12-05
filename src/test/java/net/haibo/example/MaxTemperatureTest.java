package net.haibo.example;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.MatcherAssert.*;

public class MaxTemperatureTest {
	
	@Test
	public void processValidRecord() throws IOException, InterruptedException {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + // Year ^^^^
				"99999V0203201N00261220001CN9999999N9-00111+99999999999"); // Temperature ^^^^^
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapper())
		//API Change: deprecation, withInputValue(key) --> withInput(key, value)
		.withInput(new LongWritable(1), value)
		.withOutput(new Text("1950"), new IntWritable(-11))
		.runTest();
	}
	
	@Test
	public void ignoresMissingTemperatureRecord() throws Exception {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
							          // Year ^^^^ 
				"99999V0203201N00261220001CN9999999N9+99991+99999999999");
		                              // Temperature ^^^^^
		
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapper())
		.withInput(new LongWritable(1), value)
		.runTest();
	}
	
	
	@Test
	public void returnsMaximumIntegerValue() throws IOException, InterruptedException {
		new ReduceDriver<Text,IntWritable, Text, IntWritable>()
		.withReducer(new MaxTemperatureReducer())
		//API change, deprecated withInputKey() --> withInput(key, values)
		.withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
		.withOutput(new Text("1950"), new IntWritable(10))
		.runTest();
	}
	
	
	@Test
	public void test() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		
		Path input = new Path("../hadoop-book/input/ncdc/micro");
		Path output = new Path("ouput");
		
		FileSystem fs = FileSystem.getLocal(conf);
		//API Change: deprecation, delete(output)-->delete(output, recursive)
		fs.delete(output, true);
		
		MaxTemperatureDriver driver = new MaxTemperatureDriver();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[] {input.toString(), output.toString()});
		assertThat(exitCode, is(0));
		
		//checkOutput(conf, output);
		
	}
}