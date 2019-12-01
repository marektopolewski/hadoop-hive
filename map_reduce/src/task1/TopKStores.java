package task1;
/**
 * MapReduce program performing task 'Q1- Top-k Query' from 'Project 1'.
 * Generates the first K store IDs in terms of their total net profit 
 * together with the total net profit itself.
 *
 * @author	Vlad Herghelegiu	u1600967
 * @author	Marek Topolewski	u1633084
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class TopKStores {
	
	/**
	 * The main method of the TopKStores program.
	 * @param args  - an array of Strings where: 
	 * 	args[0]	number of stores to display (k)
	 * 	args[1]	date after which all considered sales must occur
	 * 	args[2]	date before which all considered sales must occur
	 * 	args[3]	path to the input file (store_sales.dat)
	 * 	args[4]	path to the output directory
	 */
	public static void main(String[] args) throws Exception {
				
		// load into the configuration constraints from user input
		Configuration conf = new Configuration();
		int k = Integer.parseInt(args[0]),
			earlier_date = Integer.parseInt(args[1]),
			later_date = Integer.parseInt(args[2]);
		conf.setInt("k", k);
		conf.setInt("earlier_date", earlier_date);
		conf.setInt("later_date", later_date);
		
		// declare and set up a new Hadoop job
		Job job = Job.getInstance(conf, "TopKStoresJob");
		job.setJarByClass(TopKStores.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		// specify mapper and reducer classes
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
				
		// add the input path to the job
		FileInputFormat.addInputPath(job, new Path(args[3]));
		
		// clean and add the output path to the job
		Path output = new Path(args[4]);
		output.getFileSystem(job.getConfiguration()).delete(output,true);
		FileOutputFormat.setOutputPath(job, output);

		if(!job.waitForCompletion(true)) {
			System.out.println("Couldnt execute join.");
			System.out.println("Abort..\n");
			return;
		}
		
		System.out.println("Execution ended.");
		
	}
}