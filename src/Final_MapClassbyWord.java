import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

public class Final_MapClassbyWord extends Mapper<Object, Text, Text, Text> {
	private static InputStream posModelIn = null;
	private static InputStream dictLemmatizer = null;
	
	private static POSModel posModel = null;
	private static POSTaggerME posTagger = null;
	private static DictionaryLemmatizer lemmatizer = null;
	
	public Final_MapClassbyWord() {
		try {
			// ================ OpenNLP ================
	        Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			// Parts-Of-Speech Tagging
			// reading parts-of-speech model to a stream
			posModelIn = fs.open(new Path("/Twitter/models/en-pos-maxent.bin"));
			// reading parts-of-speech model to a stream
			posModel = new POSModel(posModelIn);
			// initializing the parts-of-speech tagger with model
			posTagger = new POSTaggerME(posModel);
			
			// loading the dictionary to input stream
	     	dictLemmatizer = fs.open(new Path("/Twitter/models/en-lemmatizer.txt"));
	     	// loading the lemmatizer with dictionary
			lemmatizer = new DictionaryLemmatizer(dictLemmatizer);
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String tweets = value.toString();
		String[] split_tweets = tweets.split("\\!\\@\\#\\$\\%\\^\\&\\*");
		
		// ================ Initialize ================
		// topic
//		String topic = split_tweets[0].trim();
		// username
//		String username = split_tweets[1].trim();
		// tweet
		String sentence = split_tweets[2].trim();
		// date
//		String date = split_tweets[3].trim();
		// hashtags
		String hashtags = split_tweets[4].trim();
		
		// ================ Preprocessing ================
		// ##### Handle Tweets Start #####
		String new_sentence = "";
		// toLowerCase
		sentence = sentence.toLowerCase();
		// remove &amp;
		sentence = sentence.replaceAll("\\&\\w*;", "");
		
		for (String word: sentence.split(" ")) {
			// removeURL
			if (word.length() > 1)
				word = removeRTHashtag(word);
			// removeURL
			if (word.length() > 3) 
				word = removeURLs(word);
			// cleanAllPuncNum
			// https://ascii.cl/htmlcodes.htm
			word = word.replaceAll("[\\u0021-\\u0026]", "");
			word = word.replaceAll("[\\u0028-\\u002F]", "");
			word = word.replaceAll("[\\u00A0-\\u00FF]", "");
			word = word.replaceAll("[\\u2013-\\u2122]", "");
			// only english and numbers
			word = word.replaceAll("[^A-Za-z0-9]", "");
			// remove less than two characters
			word = word.replaceAll("\\b\\w{1,2}\\b", "");
			new_sentence = (new_sentence.length() <= 0 ? word : new_sentence + " " + word);
		}
				
		// OpenNlpLemmatizer and removeStopwords
		ArrayList<String> splitwords_nlp = new ArrayList<String>();
		splitwords_nlp = OpenNlpLemmatizer(new_sentence);
		
		String word_list = "";
		for (String nlp_word: splitwords_nlp) {
			if (nlp_word != null && !nlp_word.equals("")) {
				word_list = (word_list.length() <= 0 ? nlp_word : word_list + "," + nlp_word);
			}
		}
		// ##### Handle Tweets End #####
		
		
		// ##### Handle Hashtags Start #####
		ArrayList<String> new_hashtaglist = new ArrayList<String>();
		
		hashtags = hashtags.replaceAll("[\\u005B-\\u005D]", "");
		hashtags = hashtags.toLowerCase();
		
		String[] hashtags_split = hashtags.split(",");
		
		for (int i = 0; i < hashtags_split.length; i++) {
			// cleanAllPuncNum
			// https://ascii.cl/htmlcodes.htm
			hashtags_split[i] = hashtags_split[i].replaceAll("[\\u0021-\\u002B]", "");
			hashtags_split[i] = hashtags_split[i].replaceAll("[\\u002D-\\u002F]", "");
			hashtags_split[i] = hashtags_split[i].replaceAll("[\\u00A0-\\u00FF]", "");
			hashtags_split[i] = hashtags_split[i].replaceAll("[\\u2013-\\u2122]", "");
			// only english and numbers
			hashtags_split[i] = hashtags_split[i].replaceAll("[^A-Za-z0-9]", "");
			// remove less than two characters
			hashtags_split[i] = hashtags_split[i].replaceAll("\\b\\w{1,2}\\b", "");
			
			if (!hashtags_split[i].equals("")) {
				hashtags_split[i] = hashtags_split[i].trim();
			
				// remove the same hashtag in hashtags_split
				if (!new_hashtaglist.contains(hashtags_split[i])) {
					new_hashtaglist.add(hashtags_split[i]);
				}	
			}
		}
		
		
		for (int i = 0; i < new_hashtaglist.size(); i++) {
			String topic = new_hashtaglist.get(i);
			String new_hashtags = "";
			for (int j = 0; j < new_hashtaglist.size(); j++) {
				if ( i != j )
					new_hashtags = (new_hashtags.length() <= 0 ? new_hashtaglist.get(j) : new_hashtags + "," + new_hashtaglist.get(j));
			}
			
			context.write(new Text(topic), new Text(word_list + " !@#$%^&* " + new_hashtags));
		}
		// ##### Handle Hashtags End #####
		
	}
	
	public static String removeURLs(String str) {
    	// TODO Auto-generated method stub
		if ( str != null && !str.equals("") ) {
			if (str.indexOf("http") != -1) 
				str = "";
		}
		
		return str;
	}

	public static String removeRTHashtag(String str) {
    	// TODO Auto-generated method stub
		if ( str != null && !str.equals("") ) {
			if (str.equals("rt"))
				str = "";
			else if (str.indexOf('@') != -1) 
				str = "";		
			else if (str.indexOf('#') != -1)
				str = "";
		}
    				
    	return str;
	}
	
	public static ArrayList<String> OpenNlpLemmatizer(String s) throws IOException {
		// TODO Auto-generated method stub
		ArrayList<String> result = new ArrayList<String>();
		
		// stopwords list
		String[] stopwords = {
			"about", "above", "after", "again", "against", "ain", "all", "and", "any", "are", "aren", "arent", "aren't", 
			"bio", "because", "be", "been", "before", "being", "below", "between", "both", "but", 
			"can", "couldn", "couldnt","couldn't", "could",
			"did", "didn", "didnt", "didn't", "do", "does", "doesn", "doesnt", "doesn't", "doing", "dont", "don't", "down", "during", 
			"each", 
			"few", "for", "from", "further", 
			"had", "hadn", "hadnt", "hadn't", "has", "hasn", "hasnt", "hasn't", "have", "havent", "haven't", "having", "her", "here", "hers", "herself", "him", "himself", "his", "how", "he'd", "he'll", "he's", "hes", "here's", "heres", "how's", "hows",
			"if", "into", "isn", "isnt", "isn't", "it's", "its", "itself", "i'd", "id", "i'll", "i'm", "im", "i've", "ive",
			"just", 
			"let's", "lets", "lol",
			"mightn", "mightnt", "mightn't", "more", "most", "mustn", "mustn't", "mustnt", "myself", 
			"needn", "needn't", "neednt", "nor", "not", "now", 
			"off", "once", "only", "other", "our", "ours", "ourselves", "out", "over", "own", "ought", 
			"same", "shan", "shan't", "she", "she's", "should", "should've", "shouldve", "shouldn", "shouldnt", "shouldn't", "some", "such", "she'd", "she'll", 
			"than", "that", "that'll", "thatll", "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", "through", "too", "that's", "thats", "there's", "theres", "they'd", "theyd", "they'll", "theyll", "they're", "theyre", "they've", "theyve", 
			"under", "until", 
			"very", 
			"was", "wasn", "wasn't", "wasnt", "were", "weren", "weren't",  "werent", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "won", "won't", "wont", "wouldn", "wouldn't", "wouldnt", "we'd", "we'll", "we're", "we've", "weve", "what's", "whats", "when's", "where's", "who's", "why's", "would", "whens", "wheres", "whos", "whys",
			"you", "you'd", "youd", "you'll", "youll", "you're", "youre", "you've", "youve", "your", "yours", "yourself", "yourselves"		
		};
			
			
			
		// Tokenizer
//		InputStream modelIn = fs.open(new Path("/Twitter/models/en-token.bin"));
//		TokenizerModel model = new TokenizerModel(modelIn);
//      TokenizerME tokenizer = new TokenizerME(model);
//      String tokens[] = tokenizer.tokenize(s);
		String tokens[] = s.split(" ");
		String tags[] = posTagger.tag(tokens);
		String[] lemmas = lemmatizer.lemmatize(tokens, tags);
		
		String word = "";
		            
		// printing the results
		for(int i=0;i< tokens.length;i++) {
			
			if (tags[i].equals("JJ") || tags[i].equals("JJR") || tags[i].equals("JJS") || tags[i].equals("NN") || 
				tags[i].equals("NNS") || tags[i].equals("NNP") || tags[i].equals("NNPS") || tags[i].equals("VB") || 
				tags[i].equals("VBD") || tags[i].equals("VBG") || tags[i].equals("VBN") || tags[i].equals("VBP") || 
				tags[i].equals("VBZ")) {
			
				if ( lemmas[i].equals("O")) 
					word = tokens[i];
				else
					word = lemmas[i];
				
				for( int j = 0; j < stopwords.length; j ++ ) {
		    		if (word.equals(stopwords[j])) {
		    			word = "";
		    			break;
		    		}
		    	}
					
				if (!word.equals(""))
					result.add(word);
		    }
		}       
		
		return result;
	}

}
