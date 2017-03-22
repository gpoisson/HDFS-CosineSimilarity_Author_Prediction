package offline;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class GlobalCount {
	
	ArrayList<ArrayList<WordTuple>> authors;

	public GlobalCount() {
		 authors = new ArrayList<ArrayList<WordTuple>>();
	}
	
	public int get_author_index(String authorName) {
		int author_index = -1;
		for (int i = 0; i < authors.size(); i++) {
			if (authors.get(i).get(0).word.equals(authorName)) {
				author_index = i;
				break;
			}
		}
		return author_index;
	}
	
	public void digest_line(String line) {
		String line_lc = line.toLowerCase();
		String[] line_split = line_lc.split("\t");
		String word = line_split[0];
		String author = line_split[1];
		int count = Integer.parseInt(line_split[2]);
		
		int index = get_author_index(author);
		if (index == -1) {
			WordTuple auth = new WordTuple(author, 0);
			ArrayList<WordTuple> new_author = new ArrayList<WordTuple>();
			new_author.add(auth);
			authors.add(new_author);
			index = authors.size() - 1;
		}
		WordTuple newWord = new WordTuple(word, count);
		authors.get(index).add(newWord);
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
		
		reader.close();
		
		System.out.println(gc.authors.size() + " authors found.");
		for (int i = 0; i < gc.authors.size(); i++) {
			System.out.println(" " + gc.authors.get(i).get(0).word + ":  " + (gc.authors.get(i).size() - 1 + " unique words"));
		}
		
	}
}
