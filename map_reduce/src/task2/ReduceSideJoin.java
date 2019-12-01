package task2;
/**
 * MapReduce program performing task 'Q2-Join' from 'Project 1'.
 * Generates the first K stores, with the corresponding total net profit 
 * and the employee number.
 *
 * @author	Vlad Herghelegiu	u1600967
 * @author	Marek Topolewski	u1633084
 */

import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class ReduceSideJoin {
	
	/**
	 * The main method of the ReduceSideJoin program.
	 * @param args - an array of Strings where: 
	 * 	args[0]	number of stores to display (k)
	 * 	args[1]	date after which all considered sales must occur
	 * 	args[2]	date before which all considered sales must occur
	 * 	args[3]	path to the file containing store sales (store_sales.dat)
	 * 	args[4]	path to the file containing store metadata (store.dat)
	 * 	args[5]	path to the output directory
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
		Job job = Job.getInstance(conf, "ReduceSideJoinJob");
		job.setJarByClass(ReduceSideJoin.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(JoinReducer.class);
		
		// parse input and output paths from user input
		Path inputTtopK = new Path(args[3]);
		Path inputNumEmpl = new Path(args[4]);
		Path output = new Path(args[5]);
		
		// separate input files into corresponding mapper classes
		MultipleInputs.addInputPath(job, inputTtopK, TextInputFormat.class, TopKMapper.class);
		MultipleInputs.addInputPath(job, inputNumEmpl, TextInputFormat.class, NumEmplMapper.class);
		
		// empty the output directory and pass the output path to the job
		output.getFileSystem(job.getConfiguration()).delete(output,true);
		FileOutputFormat.setOutputPath(job, output);
		
		if(!job.waitForCompletion(true)) {
			System.out.println("Couldnt execute join.");
			System.out.println("Abort..\n");
			return;
		}
		
		System.out.println("Execution ended.");
	}
	
	// Methods that allows to tokenize a String on the given character
	// and accounts for missing values, e.g. |0|1||3|4| (missing '2')
	public static ArrayList<String> tokenize(String line, char separator) {
		ArrayList<String> result = new ArrayList<>();
	  	char[] s = line.toCharArray();
	    String val = "";
	    for(int i=0; i<s.length; i++) {
	      if(s[i] == separator) {
	        result.add(val);
	        val = "";
	        continue;
	      }
	      val += s[i];
	    }
	    return result;
	  }
}