package offline;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Count the number of occurrences of a given word
public class WordCountReducer extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		int count = 0;
		
		for (@SuppressWarnings("unused") Text val: values) {
			count++;
		}
		
		context.write(key, new Text(count + ""));
	}
}