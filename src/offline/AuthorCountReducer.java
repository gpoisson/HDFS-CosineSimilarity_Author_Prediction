package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorCountReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
				
		int count = 0;
		ArrayList<String> authors = new ArrayList<String>();
		
		for (Text val: values) {
			boolean found = false;
			for (String auth: authors) {
				if (auth.equals(val.toString())){
					found = true;
				}
			}
			if (!found) {
				authors.add(val.toString());
				count++;
			}
		}
		
		context.write(new Text("author count:"), new Text(count + ""));
	}
}
