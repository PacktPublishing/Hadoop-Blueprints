package hbp.chapt1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockAverageDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	  
	  Job job = Job.getInstance();
	
	  job.setJarByClass(StockAverageMapper.class);
	
	  job.setJobName("First Job");
	
	  FileInputFormat.setInputPaths(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	  job.setMapperClass(StockAverageMapper.class);
	  job.setReducerClass(StockAverageReducer.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(DoubleWritable.class);
	
	  boolean success = job.waitForCompletion(true);
	  System.exit(success ? 0 : 1);
	};

}
