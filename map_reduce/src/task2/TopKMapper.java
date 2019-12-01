package task2;
/**
 * Mapper class for ReduceSideJoin program parsing 'store_sales.dat' file
 * If time constraints satisfied, then map each 'ss_store_sk' to 'ss_net_profit'.
 *
 * @author	Vlad Herghelegiu	u1600967
 * @author	Marek Topolewski	u1633084
 */

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class TopKMapper extends Mapper<LongWritable, Text, Text, Text>{
	     
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		// retrieve start and end date constraints from the configuration
		Configuration conf = context.getConfiguration();
		int earlier_date = conf.getInt("earlier_date", 0);
		int later_date = conf.getInt("later_date", Integer.MAX_VALUE);
		
		// split mapper's input on '|' and account for missing values
		String line = ((Text) value).toString();
		ArrayList<String> tokens = ReduceSideJoin.tokenize(line, '|');
		
		// if any of the required values missing in the record, then do not process it
		// index  	  0	      	 7	       22
		// attribute  sold_time  store_id  net_profit
		if(StringUtils.isEmpty(tokens.get(0)) || StringUtils.isEmpty(tokens.get(7)) || StringUtils.isEmpty(tokens.get(22))) {
			return;
		}
				
		// cast record's attributes into appropriate types
		Float net = Float.parseFloat(tokens.get(22));
		int date = Integer.parseInt(tokens.get(0));
		
		// process only the records within the specified time frame
		if (date>=earlier_date && date<=later_date) {
			Text store_key = new Text(tokens.get(7));
			// write to the context pair of store's key and its net profit 
			// with a label allowing it to be distinguished in the reducer
			context.write(store_key, new Text("topK," + net.toString()));
		}
	}
}
