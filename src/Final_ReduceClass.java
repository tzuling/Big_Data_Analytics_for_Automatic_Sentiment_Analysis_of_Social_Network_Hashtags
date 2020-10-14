import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Final_ReduceClass extends Reducer<Text, Text, Text, Text> {
	private static HashMap<String, String> lexicon = new HashMap<String, String>();
	private String[] lexicon_word = new String[] {};
	
	private List<StringIntPair> output = new ArrayList<StringIntPair>();
	
	public Final_ReduceClass() {
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			InputStream lexiconIn = null;
			// ================ Lexicon ================
			lexiconIn = fs.open(new Path("/Twitter/models/essay_lexicon_final.txt"));
			
			byte buffer[] = new byte[256];
			int bytesRead = 0;

			StringBuilder builder = new StringBuilder();
			while ((bytesRead = lexiconIn.read(buffer, 0, 256)) > 0) {
				String sTmp = new String(buffer, 0, bytesRead);
				builder.append(sTmp);
		  	}
			
			lexicon_word = builder.toString().split("\r\n");
			for (int i=0; i<lexicon_word.length; i++) {
				// allemotions
//				String[] a = lexicon_word[i].split(",", 2);
//				if (a[0].equals("puppet"))
//					a[1] = a[1]+",0";
//				lexicon.put(a[0], a[1]);
				// noneutral
				if (lexicon_word[i].split(",")[11].equals("0")) {
					String[] a = lexicon_word[i].split(",", 2);
					lexicon.put(a[0], a[1]);
				}
				
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		HashMap<String, Integer> wordcount = new HashMap<String, Integer>();
		HashMap<String, Integer> hashtagcount = new HashMap<String, Integer>();
		HashMap<String, String> lexicon1 = new HashMap<String, String>(lexicon);
		
		// sort wordcount
		List<StringIntPair> topic_wordcount1 = new ArrayList<StringIntPair>();
		List<StringIntPair> topic_hashtagcount1 = new ArrayList<StringIntPair>();
		
		int lexicon_sum = 0;
		Double[] twitter_emotion_p = new Double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		
		// ================ wordcount Start ================ 
		int topic_sum = 0;
		Iterator<Text> it = values.iterator();
		while(it.hasNext()){
			String line = it.next().toString();
			// word_list
			String[] wordlist = line.split("\\!\\@\\#\\$\\%\\^\\&\\*")[0].trim().split(",");
			// related_hashtag
			String[] related_hashtag = line.split("\\!\\@\\#\\$\\%\\^\\&\\*")[1].trim().split(",");
			
			// hashmap: word_list
			for (int i=0; i<wordlist.length; i++) {
				if (!wordlist[i].equals("")) {
					if (wordcount.isEmpty())
						wordcount.put(wordlist[i], 1);
					else {
						if (wordcount.containsKey(wordlist[i])) {
							int count = wordcount.get(wordlist[i]) + 1;
							wordcount.put(wordlist[i], count);
						}
						else {
							wordcount.put(wordlist[i], 1);
						}
					}
				}
			}
			
			// hashmap: related_hashtag
			for (int i=0; i<related_hashtag.length; i++) {
				if (!related_hashtag[i].equals("")) {
					if (hashtagcount.isEmpty())
						hashtagcount.put(related_hashtag[i], 1);
					else {
						if (hashtagcount.containsKey(related_hashtag[i])) {
							int count = hashtagcount.get(related_hashtag[i]) + 1;
							hashtagcount.put(related_hashtag[i], count);
						}
						else {
							hashtagcount.put(related_hashtag[i], 1);
						}
					}
				}
			}
			
			topic_sum = topic_sum + 1;
		}
		// ================ wordcount End ================ 
		
		
		if (output.size() < 50 || topic_sum > output.get(output.size()-1).getInteger()) {
			// ================ Sentiment analysis and store in StringIntPair Start ================
			// word_list: sentiment analysis and store in topic_wordcount1
			for (Object wordcount_key : wordcount.keySet()) {
				if (lexicon1.containsKey(wordcount_key)) {
					// topic_lexicon count
					String[] lexicon_line_split = lexicon1.get(wordcount_key).split(",");
					
					int count = Integer.parseInt(lexicon_line_split[11]) + wordcount.get(wordcount_key);
					
					String newline = lexicon_line_split[0] + "," + lexicon_line_split[1] + "," + lexicon_line_split[2] + "," 
								+ lexicon_line_split[3] + "," + lexicon_line_split[4] + "," + lexicon_line_split[5] + "," 
								+ lexicon_line_split[6] + "," + lexicon_line_split[7] + "," + lexicon_line_split[8] + "," 
								+ lexicon_line_split[9] + "," + lexicon_line_split[10] + "," + count;
					
					if (! (lexicon_line_split[0].equals("0") && lexicon_line_split[1].equals("0") && lexicon_line_split[2].equals("0") &&
						lexicon_line_split[3].equals("0") && lexicon_line_split[4].equals("0") && lexicon_line_split[5].equals("0") &&
						lexicon_line_split[6].equals("0") && lexicon_line_split[7].equals("0") )){
							lexicon_sum = lexicon_sum + count;
					}
					
					lexicon1.put(wordcount_key.toString(), newline);
				}
	
				topic_wordcount1.add(new StringIntPair(wordcount_key.toString(), wordcount.get(wordcount_key)));	
			}
			
			// related_hashtag: topic_hashtagcount1
			for (Object hashtagcount_key : hashtagcount.keySet()) {
				topic_hashtagcount1.add(new StringIntPair(hashtagcount_key.toString(), hashtagcount.get(hashtagcount_key)));	
			}
			// ================ Sentiment analysis and store in StringIntPair End ================
	
			// 11 emotions total
			for (Object lexicon_key : lexicon1.keySet()) {
				String line = lexicon1.get(lexicon_key);
				String[] lexicon_line = line.split(",");
			
				double probability = Double.parseDouble(lexicon_line[11]) / (double) lexicon_sum;
				
				for (int i=0; i < twitter_emotion_p.length; i ++) {
					if (i<=7) {
						if ( !lexicon_line[i].equals("0") ) {
							twitter_emotion_p[i] = twitter_emotion_p[i] + Double.parseDouble(lexicon_line[i]) * probability;
						}
					}
					else {
						if ( !lexicon_line[i].equals("0") ) {
							twitter_emotion_p[i] = twitter_emotion_p[i] + Double.parseDouble(lexicon_line[i]) * Double.parseDouble(lexicon_line[11]);
						}
					}
//					if (i <= 7) {
//						if ( !lexicon_line[i].contentEquals("0") ) {
//							probability = Double.parseDouble(lexicon_line[11]) / (double) lexicon_sum;
//						}
//					}
//					twitter_emotion_p[i] = twitter_emotion_p[i] + Double.parseDouble(lexicon_line[i]) * probability;
				}
				
			}
			
			// print sentiment analysis
			String sa_value = 
					"angry emotion probability: " + twitter_emotion_p[0] + "\n" +
					"anticipation emotion probability: " + twitter_emotion_p[1] + "\n" +
					"disgust emotion probability: "	+ twitter_emotion_p[2] + "\n" +
					"fear emotion probability: " + twitter_emotion_p[3] + "\n" +
					"joy emotion probability: " + twitter_emotion_p[4] + "\n" +
					"sadness emotion probability: " + twitter_emotion_p[5] + "\n" +
					"surprise emotion probability: " + twitter_emotion_p[6] + "\n" +
					"trust emotion probability: " + twitter_emotion_p[7] + "\n" +
					"negative sentiment total: " + twitter_emotion_p[8] + "\n" +
					"positive sentiment total: " + twitter_emotion_p[9] + "\n" +
					"neutral sentiment total: " + twitter_emotion_p[10] ;
			
			// ================ Integer Sorted and Print Start ================ 
			// word_list
			Collections.sort(topic_wordcount1,
	        new Comparator<StringIntPair>() {
	            public int compare(StringIntPair o1, StringIntPair o2) {
	                return o2.getInteger()-o1.getInteger();
	            }
	        });
			
			String wc_value = "";
			for (int i=0; i < 100; i++) {
				if ( i >= topic_wordcount1.size() )
					break;
				if (!topic_wordcount1.get(i).equals(""))
					wc_value = (wc_value.length() <= 0 ? "WordCount: " + "\n" + topic_wordcount1.get(i) : wc_value + " " + topic_wordcount1.get(i));
	
			}
			
			// related_hashtag
			Collections.sort(topic_hashtagcount1,
	        new Comparator<StringIntPair>() {
	            public int compare(StringIntPair o1, StringIntPair o2) {
	                return o2.getInteger()-o1.getInteger();
	            }
	        });
			
			String hashtag_value = "";
			for (int i=0; i < 100; i++) {
				if ( i >= topic_hashtagcount1.size() )
					break;
				if (!topic_hashtagcount1.get(i).equals("")) {
					double count = (double)topic_hashtagcount1.get(i).getInteger() / (double)topic_sum;
					String value = topic_hashtagcount1.get(i).getString()+","+count;
					hashtag_value = (hashtag_value.length() <= 0 ? "Hashtags WordCount: " + "\n" + value : hashtag_value + " " + value);
				}
			}
			// ================ Integer Sorted and Print End ================ 
			
			// ================ Output Top 50 and Sorted Start ================
			String allvalue = key.toString().trim() + "\n" + sa_value + "\n" + wc_value + "\n" + hashtag_value + "\n";
			output.add(new StringIntPair(allvalue, topic_sum));
			Collections.sort(output,
	        new Comparator<StringIntPair>() {
	            public int compare(StringIntPair o1, StringIntPair o2) {
	                return o2.getInteger()-o1.getInteger();
	            }
	        });
			
			if (output.size() > 50)
				output.remove(output.size()-1);
			// ================ Output Top 50 and Sorted End ================
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		int i = 1;
		for (Object o:output) {
			context.write(new Text("Top "+ i + ": \n"), new Text(""+ o));
			i ++;
		}
	}
}
