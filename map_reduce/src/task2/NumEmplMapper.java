package task2;
/**
 * Mapper class for ReduceSideJoin program parsing 'store.dat' file
 * Map each store id 's_store_sk' to 's_number_employees'
 *
 * @author	Vlad Herghelegiu	u1600967
 * @author	Marek Topolewski	u1633084
 */

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumEmplMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		// split mapper's input on '|' and account for missing values
		String line = ((Text) value).toString();
		ArrayList<String> tokens = ReduceSideJoin.tokenize(line, '|');
		
		// if any of the required values missing in the record, then do not process it
		// index  	  0	        6
		// attribute  store_id  num_employees
		if(StringUtils.isEmpty(tokens.get(0)) || StringUtils.isEmpty(tokens.get(6))) {
			return;
		}
		
		// cast record's attributes into appropriate types
		Text store_key = new Text(tokens.get(0));
		Float numOfEmpl = Float.parseFloat(tokens.get(6));
		
		// write to the context pair of store's key and employee count 
		// with a label allowing it to be distinguished in the reducer
		context.write(store_key, new Text("empl,"+numOfEmpl));
	}

}
