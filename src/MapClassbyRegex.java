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
 
public class MapClassbyRegex extends Mapper<Object, Text, Text, Text> {

//	private Text mapkey = new Text();
//	private Text mapvalue = new Text();
	
	private static InputStream posModelIn = null;
	private static InputStream dictLemmatizer = null;
//	private static InputStream lexicon = null;
	
	private static POSModel posModel = null;
	private static POSTaggerME posTagger = null;
	private static DictionaryLemmatizer lemmatizer = null;

//	private static BufferedReader lexicon_read_line = null;
	
//	private static HashMap<String, ArrayList<String>> hmap_lexicon = null;
//	private static ArrayList<String> lexicon_list = null;
	
//	private static HashMap<String, ArrayList<String>> hmap_wordcount = null;
// 	ArrayList<String> wordcount_list = null;
		
	public MapClassbyRegex() {
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
 			
	     	// ================ Lexicon ================
//	     	lexicon = fs.open(new Path("/Twitter/models/essay_lexicon_final.txt"));
//	     	lexicon_read_line = new BufferedReader(new InputStreamReader(lexicon));
	     	// ================ HMap ===================
	     	// hmap: key: topic, value: lexicon
//	    	hmap_lexicon = new HashMap<String, ArrayList<String>>();
//	    	hmap_wordcount = new HashMap<String, ArrayList<String>>();
	    	
//	    	String sentence = "";
//	    	lexicon_list = new ArrayList<String>();
//	    	while((sentence = lexicon_read_line.readLine()) != null ) {
//	     		lexicon_list.add(sentence);
//			}
	    	
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	
		String tweets = value.toString();
		
		String[] split_tweets = tweets.split("\\!\\@\\#\\$\\%\\^\\&\\*");
		
		// topic
		String topic = split_tweets[0].trim();
		// username
//		String username = split_tweets[1].trim();
		// tweet
		String sentence = split_tweets[2].trim();
		// date
//		String date = split_tweets[3].trim();
		// hashtags
//		String hashtags = split_tweets[4].trim();
   
     	
     	// create hmap_lexicon <topic, emotion list> 
//		if (hmap_lexicon.isEmpty()) {
//			// create hash map
//			hmap_lexicon.put(topic, lexicon_list);
//		} else {
//			if (!hmap_lexicon.containsKey(topic)) {
//				// create hash map
//				hmap_lexicon.put(topic, lexicon_list);
//			}
//		}
		
		// create hmap_wordcount <topic, wordcount list>
//		if (hmap_wordcount.isEmpty()) {
//			wordcount_list = new ArrayList<String>();
//			hmap_wordcount.put(topic, wordcount_list);
//		} else {
//			if (!hmap_lexicon.containsKey(topic)) {
//				wordcount_list = new ArrayList<String>();
//				hmap_wordcount.put(topic, wordcount_list);
//			}
//		}
		
		// ============ preprocessing ============
		
		// toLowerCase
		sentence = sentence.toLowerCase();
		
		// removeURL
		sentence = sentence.replaceAll("https?:\\/\\/(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b([-a-zA-Z0-9()@:%_\\+.~#?&//=]*)","");
		sentence = sentence.replaceAll("[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b([-a-zA-Z0-9()@:%_\\+.~#?&//=]*)", "");
		
		// remove &amp;
		sentence = sentence.replaceAll("\\&\\w*;", "");
		
		// removeRTHashtag
		sentence = sentence.replaceAll("#[A-Za-z0-9\\-\\.\\_]+(?:\\s|$)", "");
		sentence = sentence.replaceAll("@[A-Za-z0-9\\-\\.\\_]+(?:\\s|$)", "");
		
		// cleanAllPuncNum
		// https://ascii.cl/htmlcodes.htm
		sentence = sentence.replaceAll("[\\u0021-\\u0026]", "");
		sentence = sentence.replaceAll("[\\u0028-\\u002F]", "");
		sentence = sentence.replaceAll("[\\u00A0-\\u00FF]", "");
		sentence = sentence.replaceAll("[\\u2013-\\u2122]", "");

		// only english and numbers
		sentence = sentence.replaceAll("[^A-Za-z0-9]", " ");
		
		// remove less than two characters
		sentence = sentence.replaceAll("\\b\\w{1,2}\\b", "");
				
		// OpenNlpLemmatizer and removeStopwords
		ArrayList<String> splitwords_nlp = new ArrayList<String>();
		splitwords_nlp = OpenNlpLemmatizer(sentence);
		
		String word_list = "";
		for (String nlp_word: splitwords_nlp) {
			if (nlp_word != null && !nlp_word.equals("")) {
				word_list = (word_list.length() <= 0 ? nlp_word : word_list + "," + nlp_word);
			}
		}
		
		context.write(new Text(topic), new Text(word_list));
		// count emotion list words
//		String line = "", newline = "";
//		int count = 0;
//		int rewrite = 0;
//		ArrayList<String> hmap_lexicon_value = new ArrayList<String>();
//		ArrayList<String> hmap_wordcount_value = new ArrayList<String>();
		
//		hmap_lexicon_value = hmap_lexicon.get(topic);
//		hmap_wordcount_value = hmap_wordcount.get(topic);
		
//		for (String nlp_word: splitwords_nlp) {
//			if (nlp_word != null && !nlp_word.equals("")) {
//				for (int j = 0; j < lexicon_list.size(); j++) {
//					line = lexicon_list.get(j);
//					
//					if (nlp_word.equals(line.split(",")[0])){
//						rewrite = 1;
//						count = Integer.parseInt(line.split(",")[12]) + 1;
//						
//						newline = line.split(",")[0] + "," + line.split(",")[1] + "," + line.split(",")[2] + "," 
//								+ line.split(",")[3] + "," + line.split(",")[4] + "," + line.split(",")[5] + "," 
//								+ line.split(",")[6] + "," + line.split(",")[7] + "," + line.split(",")[8] + "," 
//								+ line.split(",")[9] + "," + line.split(",")[10] + "," + line.split(",")[11] + "," 
//								+ "1";
//
//						hmap_lexicon_value.set(j, newline);
//						context.write(new Text(topic), new Text(newline));
//					} 
//					else {
//						String newadd = nlp_word + ",1";
//						wordcount_list.add(newadd);
//						hmap_wordcount.put(topic, wordcount_list);
//						context.write(new Text(topic), new Text(newadd));
//					}
//				}
//			}
//		}
		
//		if (rewrite == 1) {
//			hmap_lexicon.put(topic, hmap_lexicon_value);
//			rewrite = 0;
//		}
		
		// map(key:topic, value:one line of lexicon)
//		for (Object hmap_key : hmap_lexicon.keySet()) {
//			hmap_lexicon_value = hmap_lexicon.get(hmap_key);
//			
//			for (int i=0; i < hmap_lexicon_value.size(); i++) {
//				mapkey.set(hmap_key.toString());
//				mapvalue.set(hmap_lexicon_value.get(i).toString());
//				context.write(new Text(mapkey), new Text(mapvalue));
//			}
//
//        }
		
		// map(key:topic, value:one line of lexicon)
//		for (Object hmap_key : hmap_wordcount.keySet()) {
//			wordcount_list = hmap_wordcount.get(hmap_key);
//			
//			for (int i=0; i < wordcount_list.size(); i++) {
//				mapkey.set((String) hmap_key);
//				mapvalue.set(hmap_wordcount.get(hmap_key).get(i));
//				context.write(new Text(mapkey), new Text(mapvalue));
//			}
//
//		}	
	}
	
	public static ArrayList<String> OpenNlpLemmatizer(String s) throws IOException {
		// TODO Auto-generated method stub
		ArrayList<String> result = new ArrayList<String>();
		
		// stopwords list
		String[] stopwords = {
			"about", "above", "after", "again", "against", "ain", "all", "and", "any", "are", "aren", "aren't", 
			"bio", "because", "been", "before", "being", "below", "between", "both", "but", 
			"can", "couldn", "couldn't", "could",
			"did", "didn", "didn't", "does", "doesn", "doesn't", "doing", "don", "don't", "down", "during", 
			"each", 
			"few", "for", "from", "further", 
			"had", "hadn", "hadn't", "has", "hasn", "hasn't", "have", "haven", "haven't", "having", "her", "here", "hers", "herself", "him", "himself", "his", "how", "he'd", "he'll", "he's", "here's", "how's",
			"if", "into", "isn", "isn't", "it's", "its", "itself", "i'd", "i'll", "i'm", "i've", 
			"just", 
			"let's", "lol",
			"mightn", "mightn't", "more", "most", "mustn", "mustn't", "myself", 
			"needn", "needn't", "nor", "not", "now", 
			"off", "once", "only", "other", "our", "ours", "ourselves", "out", "over", "own", "ought", 
			"same", "shan", "shan't", "she", "she's", "should", "should've", "shouldn", "shouldn't", "some", "such", "she'd", "she'll", 
			"than", "that", "that'll", "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", "through", "too", "that's", "there's", "they'd", "they'll", "they're", "they've", 
			"under", "until", 
			"very", 
			"was", "wasn", "wasn't", "were", "weren", "weren't", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "won", "won't", "wouldn", "wouldn't", "we'd", "we'll", "we're", "we've", "what's", "when's", "where's", "who's", "why's", "would",
			"you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"		
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