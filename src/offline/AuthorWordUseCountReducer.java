package offline;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Count the number of authors who use a specific word
public class AuthorWordUseCountReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		int count = 0;
		
		for (@SuppressWarnings("unused") Text val: values) {
			count++;
		}
		if (key != null && count > 0) {
			String term = key.toString();
			context.write(new Text(term), new Text(count + ""));
		}
	}
}
