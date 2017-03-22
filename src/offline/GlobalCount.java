package offline;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class GlobalCount {
	
	ArrayList<ArrayList<WordTuple>> authors;

	public GlobalCount() {
		 authors = new ArrayList<ArrayList<WordTuple>>();
	}
	
	public int get_author_index(String authorName, GlobalCount globalCount) {
		int author_index = -1;
		for (int i = 0; i < globalCount.authors.size(); i++) {
			if (globalCount.authors.get(i).get(0).word.equals(authorName)) {
				author_index = i;
				break;
			}
		}
		return author_index;
	}
	
	public void digest_line(String line) {
		String line_lc = line.toLowerCase();
	}
	
	public static void main(String[] args) throws IOException {
		String mrWordcountFilename = "part-r-00000";
		BufferedReader reader;
		GlobalCount gc = new GlobalCount();
		
		reader = new BufferedReader(new FileReader(mrWordcountFilename));
		String line;
		
		while((line = reader.readLine()) != null) {
			gc.digest_line(line);
		}
		
	}
}
