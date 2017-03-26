package offline;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// List author names
public class AuthorNamesReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
				
		int count = 0;
		
		for (@SuppressWarnings("unused") Text val: values) {
			count++;
		}
		
		String author = key.toString();
		
		context.write(new Text(author), new Text(count + ""));
	}
}
