package offline;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class GlobalCount {
	
	ArrayList<ArrayList<WordTuple>> authors;
	ArrayList<WordTuple> all_words_by_authors;

	public GlobalCount() {
		 authors = new ArrayList<ArrayList<WordTuple>>();
		 all_words_by_authors = new ArrayList<WordTuple>();
	}
	
	private int get_author_index(String authorName) {
		int author_index = -1;
		for (int i = 0; i < authors.size(); i++) {
			if (authors.get(i).get(0).word.equals(authorName)) {
				author_index = i;
				break;
			}
		}
		return author_index;
	}
	
	private void digest_line(String line) {
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
		new_author_uses_word(newWord);
		authors.get(index).add(newWord);
	}
	
	public void new_author_uses_word(WordTuple word) {
		boolean found = false;
		WordTuple known_word;
		if (all_words_by_authors.size() > 0) {
			known_word = all_words_by_authors.get(all_words_by_authors.size()-1);
			if (known_word.word.equals(word.word)) {
				known_word.count++;
				found = true;
			}
			if (!found) {
				WordTuple unique_word = new WordTuple(word.word, 1);
				all_words_by_authors.add(unique_word);
			}
		}
		else {
			known_word = new WordTuple(word.word, 1);
			all_words_by_authors.add(known_word);
		}
	}
	
	public WordTuple most_common_word(int author_index) {
		int greatest_occurrences = 0;
		String most_common = new String();
		for (int i = 1; i < authors.get(author_index).size(); i++) {
			if (authors.get(author_index).get(i).count > greatest_occurrences) {
				greatest_occurrences = authors.get(author_index).get(i).count;
				most_common = authors.get(author_index).get(i).word;
			}
		}
		WordTuple mc = new WordTuple(most_common, greatest_occurrences);
		return mc;
	}
	
	public int number_of_authors_who_use_term(String term) {
		int author_count = 0;
		for (WordTuple known: all_words_by_authors) {
			if (known.word.equals(term)) {
				author_count = known.count;
				break;
			}
		}		
		return author_count;
	}
	
	public void build_global_word_count() throws IOException {
		String mrWordcountFilename = "part-r-00000";
		BufferedReader reader;
				
		reader = new BufferedReader(new FileReader(mrWordcountFilename));
		String line;
		
		while((line = reader.readLine()) != null) {
			digest_line(line);
		}
		
		reader.close();
	}
	
	public static void main(String[] args) throws IOException {
		
		GlobalCount gc = new GlobalCount();
		System.out.println(gc.authors.size() + " authors found.");
		for (int i = 0; i < gc.authors.size(); i++) {
			System.out.println(" " + gc.authors.get(i).get(0).word + ":  " + (gc.authors.get(i).size() - 1 + " unique words"));
		}		
	}
}
