package task2;
/**
 * Reducer class for ReduceSideJoin program
 * Takes pairs generated by both mappers and outputs a list of
 * top K stores with their corresponding employee count.
 *
 * @author	Vlad Herghelegiu	u1600967
 * @author	Marek Topolewski	u1633084
 */

import java.io.IOException;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    // TreeMap structure ordered by its key, at any state stores up to K stores with highest net profit
    private TreeMap<FloatWritable, Text> topStores = new TreeMap<>();
    	
    // HashMap structure, stores key-value pairs generated by the NumEmplMapper mappers
    private HashMap<Text, Text> numEmpl = new HashMap<>();
    
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
		// current sum of net profit for the store with key 'key'
		float sum = 0;
		// flag to indicate whether any record has been generated by TopKMapper mappers for this key
		boolean added = false;

		// iterate through all values for the given store generated by either of the mappers
		for (Text value : values) {
			// split every value on ',' to detect the label from NumEmplMapper pairs
			String[] line = value.toString().split(",");
			if (line[0].equals("empl")) {
				numEmpl.put(new Text(key), new Text(line[1]));
			}
			// if no 'empl' label, then the value originates from the TopKMapper,
			// hence, use it to calculate the summary net profit
			else {
				sum += Float.parseFloat(line[1]);
				// indicate that at least one record processed by the TopKMapper
				added = true;
			}
		}
		
		// do not add to the tree entries of stores with no net profit records
		// excludes stores with records in store_data with not in store store_sales
		if (!added) return;
		
		// retrieve the max number of stores to display (k)
		int k = context.getConfiguration().getInt("k", 1);

		// add to the tree a pair of the net profit and the corresponding store id
		// this order allows for auto-balancing of the tree based on the profit
		topStores.put(new FloatWritable(sum), new Text(key));
		
		// if tree size exceeds k, remove the pair with the smallest net profit
		if (topStores.size() > k) topStores.remove(topStores.firstKey());
	}
	
	@Override
	// method executed once, after reducers are finished and before the destructor
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		// Create a temporary Tree map to sort the top K stores within topStores TreeMap
		// Integers used as key to ensure correct order (for Text the order is lexicographic)
		TreeMap<Integer, FloatWritable> stores = new TreeMap<>();
		for(Map.Entry<FloatWritable, Text> entry : topStores.entrySet()) {
			stores.put(new Integer(entry.getValue().toString()), entry.getKey());
		}
		
		// write each remaining entry to the output (TreeMap ensures the ordering)
		for(Map.Entry<Integer, FloatWritable> entry : stores.entrySet()) {
			FloatWritable net = entry.getValue();
			Text store_id = new Text(entry.getKey().toString());
			Text empl = numEmpl.getOrDefault(store_id, new Text("N/A"));
			context.write(store_id, new Text(net + "\t" + empl));
		}

		// call the default 'cleanup' method for the Reducers
		super.cleanup(context);
	}
}

