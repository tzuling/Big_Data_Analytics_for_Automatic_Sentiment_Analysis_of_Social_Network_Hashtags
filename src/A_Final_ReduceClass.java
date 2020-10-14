import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class A_Final_ReduceClass extends Reducer<Text, Text, Text, Text> {
	private static HashMap<String, String> lexicon = new HashMap<String, String>();
	private String[] lexicon_word = new String[] {};
	
	private List<StringIntPair> output = new ArrayList<StringIntPair>();
	private HashMap<String, String> week_output = new HashMap<String, String>();
	private ArrayList<Integer> week = new ArrayList<Integer>();
	
	public static final Log log = LogFactory.getLog(A_Final_ReduceClass.class);
	
	public A_Final_ReduceClass() {
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
	
		int weeks = 26;
		HashMap<Integer, Integer> week_tweet_sum = new HashMap<Integer, Integer>();
		HashMap<Integer, HashMap<String, Integer>> week_wordcount = new HashMap<Integer, HashMap<String, Integer>>();
		HashMap<Integer, HashMap<String, Integer>> week_hashtagcount = new HashMap<Integer, HashMap<String, Integer>>();
		
		for(int i=1; i<=weeks; i++) {
			week_tweet_sum.put(i, 0);
			week_wordcount.put(i, new HashMap<String, Integer>());
			week_hashtagcount.put(i, new HashMap<String, Integer>());
		}
		
		HashMap<String, Integer> wordcount = new HashMap<String, Integer>();
		HashMap<String, Integer> hashtagcount = new HashMap<String, Integer>();
		
		log.info(key + "\n");
		System.out.println(key.toString());
		
		// ================ wordcount Start ================ 
		int topic_sum = 0;
		Iterator<Text> it = values.iterator();
		while(it.hasNext()){
			// ================ get word_list, related_hashtag, weekofyear ================
			String line = it.next().toString();
			// word_list
			String[] wordlist = line.split("\\!\\@\\#\\$\\%\\^\\&\\*")[0].trim().split(",");
			// related_hashtag
			String[] related_hashtag = line.split("\\!\\@\\#\\$\\%\\^\\&\\*")[1].trim().split(",");
			// weekofyear
			Integer weekofyear = Integer.parseInt(line.split("\\!\\@\\#\\$\\%\\^\\&\\*")[2].trim());
			
			
			// ================ count weekofyear and total wordcount ================
			for (int i=0; i<wordlist.length; i++) {
				if (!wordlist[i].equals("")) {
					// week wordcount
					HashMap<String, Integer> tmp_week_wordcount = week_wordcount.get(weekofyear);
					if (tmp_week_wordcount.isEmpty())
						tmp_week_wordcount.put(wordlist[i], 1);
					else {
						if (tmp_week_wordcount.containsKey(wordlist[i])) {
							int count = tmp_week_wordcount.get(wordlist[i]) + 1;
							tmp_week_wordcount.put(wordlist[i], count);
						}
						else {
							tmp_week_wordcount.put(wordlist[i], 1);
						}
					}
					week_wordcount.put(weekofyear, tmp_week_wordcount);
					
					// total wordcount
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
			
			
			// ================ count weekofyear and total hashtagcount ================
			for (int i=0; i<related_hashtag.length; i++) {
				if (!related_hashtag[i].equals("")) {
					// week hashtagcount
					HashMap<String, Integer> tmp_week_hashtagcount = week_hashtagcount.get(weekofyear);
					if (tmp_week_hashtagcount.isEmpty())
						tmp_week_hashtagcount.put(related_hashtag[i], 1);
					else {
						if (tmp_week_hashtagcount.containsKey(related_hashtag[i])) {
							int count = tmp_week_hashtagcount.get(related_hashtag[i]) + 1;
							tmp_week_hashtagcount.put(related_hashtag[i], count);
						}
						else {
							tmp_week_hashtagcount.put(related_hashtag[i], 1);
						}
					}
					week_hashtagcount.put(weekofyear, tmp_week_hashtagcount);
					
					// total hashtagcount
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
			int count = week_tweet_sum.get(weekofyear) +1;
			week_tweet_sum.put(weekofyear, count);
			topic_sum = topic_sum + 1;
		}
		// ================ wordcount End ================ 
		
		if (output.size() < 10 || topic_sum > output.get(output.size()-1).getInteger()) {
			List<StringIntPair> sort_wordcount = new ArrayList<StringIntPair>();
			List<StringIntPair> sort_hashtagcount = new ArrayList<StringIntPair>();
			
			Double[] emotion_p = new Double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
			
			
			for(int i=1; i<=weeks; i++) {
				
				if ( week_tweet_sum.get(i) > 0) {
					
					HashMap<String, String> lexicon1 = new HashMap<String, String>(lexicon);
					List<StringIntPair> sort_week_wordcount = new ArrayList<StringIntPair>();
					List<StringIntPair> sort_week_hashtagcount = new ArrayList<StringIntPair>();
					Double[] week_emotion_p = new Double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
					int lexicon_sum = 0;
					
					HashMap<String, Integer> tmp_week_wordcount = week_wordcount.get(i);
					HashMap<String, Integer> tmp_week_hashtagcount = week_hashtagcount.get(i);
					
					if (!week.contains(i))
						week.add(i);
					
					// ================ Sentiment analysis and store in StringIntPair Start ================
					// word_list: sentiment analysis and store in topic_wordcount
					for (Object wordcount_key : tmp_week_wordcount.keySet()) {
						if (lexicon1.containsKey(wordcount_key)) {
							// topic_lexicon count
							String[] lexicon_line_split = lexicon1.get(wordcount_key).split(",");
							
							int count = Integer.parseInt(lexicon_line_split[11]) + tmp_week_wordcount.get(wordcount_key);
							
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
	
						sort_week_wordcount.add(new StringIntPair(wordcount_key.toString(), tmp_week_wordcount.get(wordcount_key)));	
					}
					
					// ================ Sentiment analysis and store in StringIntPair End ================
					// 11 emotions total
					for (Object lexicon_key : lexicon1.keySet()) {
						String line = lexicon1.get(lexicon_key);
						String[] lexicon_line = line.split(",");

//						lexicon_sum = (lexicon_sum == 0) ? 1 : lexicon_sum;
						double probability = Double.parseDouble(lexicon_line[11]) / (double) lexicon_sum;
						
						for (int j=0; j < week_emotion_p.length; j ++) {
							if (j <= 7) {
								if ( !lexicon_line[j].equals("0") ) {
									week_emotion_p[j] = week_emotion_p[j] + Double.parseDouble(lexicon_line[j]) * probability;
								}
							}
							else {
								if ( !lexicon_line[j].equals("0") ) {
									week_emotion_p[j] = week_emotion_p[j] + Double.parseDouble(lexicon_line[j]) * Double.parseDouble(lexicon_line[11]);
								}
							}
						}
					}
					
					// print sentiment analysis
					String sa_value = 
						"angry emotion probability: " + formatDouble4(week_emotion_p[0]) + "\n" +
						"anticipation emotion probability: " + formatDouble4(week_emotion_p[1]) + "\n" +
						"disgust emotion probability: "	+ formatDouble4(week_emotion_p[2]) + "\n" +
						"fear emotion probability: " + formatDouble4(week_emotion_p[3]) + "\n" +
						"joy emotion probability: " + formatDouble4(week_emotion_p[4]) + "\n" +
						"sadness emotion probability: " + formatDouble4(week_emotion_p[5]) + "\n" +
						"surprise emotion probability: " + formatDouble4(week_emotion_p[6]) + "\n" +
						"trust emotion probability: " + formatDouble4(week_emotion_p[7]) + "\n" +
						"negative sentiment total: " + formatDouble4(week_emotion_p[8]) + "\n" +
						"positive sentiment total: " + formatDouble4(week_emotion_p[9]);
	
					for (int j = 0; j < emotion_p.length; j++) {
						if (week_emotion_p[j].isNaN())
							continue;
						
						if (j <= 7) {
							emotion_p[j] = emotion_p[j] + week_emotion_p[j] * week_tweet_sum.get(i) / topic_sum;
						}
						else {
							emotion_p[j] = emotion_p[j] + week_emotion_p[j];
						}
					}
					
					// ================ Integer Sorted and Print Start ================ 
					// word_list
					Collections.sort(sort_week_wordcount,
			        new Comparator<StringIntPair>() {
			            public int compare(StringIntPair o1, StringIntPair o2) {
			                return o2.getInteger()-o1.getInteger();
			            }
			        });
					
					String wc_value = "";
					for (int j=0; j < 100; j++) {
						if ( j >= sort_week_wordcount.size() )
							break;
						if (!sort_week_wordcount.get(j).equals(""))
							wc_value = (wc_value.length() <= 0 ? "WordCount: " + "\n" + sort_week_wordcount.get(j) : wc_value + " " + sort_week_wordcount.get(j));
			
					}
					
					// related_hashtag: topic_hashtagcount1
					for (Object hashtagcount_key : tmp_week_hashtagcount.keySet()) {
						sort_week_hashtagcount.add(new StringIntPair(hashtagcount_key.toString(), tmp_week_hashtagcount.get(hashtagcount_key)));	
					}
	
					// related_hashtag
					Collections.sort(sort_week_hashtagcount,
			        new Comparator<StringIntPair>() {
			            public int compare(StringIntPair o1, StringIntPair o2) {
			                return o2.getInteger()-o1.getInteger();
			            }
			        });
					
					String hashtag_value = "";
					for (int j=0; j < 100; j++) {
						if ( j >= sort_week_hashtagcount.size() )
							break;
						if (!sort_week_hashtagcount.get(j).equals("")) {
							double count = (double)sort_week_hashtagcount.get(j).getInteger() / (double)topic_sum;
							String value = sort_week_hashtagcount.get(j).getString()+","+formatDouble4(count);
							hashtag_value = (hashtag_value.length() <= 0 ? "Hashtags WordCount: " + "\n" + value : hashtag_value + " " + value);
						}
					}
	
					// ================ Integer Sorted and Print End ================
					
					// ================ Week Output ================
					String week_allvalue = sa_value + "\n" + wc_value + "\n" + hashtag_value + "\n";
					week_output.put(key.toString().trim()+"_"+i, week_allvalue);
					
					log.info(week_output.get(key.toString().trim()+"_"+i));
					System.out.println(week_output.get(key.toString().trim()+"_"+i));
				}
			}
			
			// print sentiment analysis
			String sa_value = 
				"angry emotion probability: " + formatDouble4(emotion_p[0]) + "\n" +
				"anticipation emotion probability: " + formatDouble4(emotion_p[1]) + "\n" +
				"disgust emotion probability: "	+ formatDouble4(emotion_p[2]) + "\n" +
				"fear emotion probability: " + formatDouble4(emotion_p[3]) + "\n" +
				"joy emotion probability: " + formatDouble4(emotion_p[4]) + "\n" +
				"sadness emotion probability: " + formatDouble4(emotion_p[5]) + "\n" +
				"surprise emotion probability: " + formatDouble4(emotion_p[6]) + "\n" +
				"trust emotion probability: " + formatDouble4(emotion_p[7]) + "\n" +
				"negative sentiment total: " + formatDouble4(emotion_p[8]) + "\n" +
				"positive sentiment total: " + formatDouble4(emotion_p[9]);
			
			// word_list
			for (Object wordcount_key : wordcount.keySet()) {
				sort_wordcount.add(new StringIntPair(wordcount_key.toString(), wordcount.get(wordcount_key)));
			}
			
			Collections.sort(sort_wordcount,
	        new Comparator<StringIntPair>() {
	            public int compare(StringIntPair o1, StringIntPair o2) {
	                return o2.getInteger()-o1.getInteger();
	            }
	        });
			
			String wc_value = "";
			for (int j=0; j < 100; j++) {
				if ( j >= sort_wordcount.size() )
					break;
				if (!sort_wordcount.get(j).equals(""))
					wc_value = (wc_value.length() <= 0 ? "WordCount: " + "\n" + sort_wordcount.get(j) : wc_value + " " + sort_wordcount.get(j));
	
			}
			
			// related_hashtag
			for (Object hashtagcount_key : hashtagcount.keySet()) {
				sort_hashtagcount.add(new StringIntPair(hashtagcount_key.toString(), hashtagcount.get(hashtagcount_key)));	
			}
			
			Collections.sort(sort_hashtagcount,
	        new Comparator<StringIntPair>() {
	            public int compare(StringIntPair o1, StringIntPair o2) {
	                return o2.getInteger()-o1.getInteger();
	            }
	        });
			
			String hashtag_value = "";
			for (int j=0; j < 100; j++) {
				if ( j >= sort_hashtagcount.size() )
					break;
				if (!sort_hashtagcount.get(j).equals("")) {
					double count = (double)sort_hashtagcount.get(j).getInteger() / (double)topic_sum;		
					String value = sort_hashtagcount.get(j).getString()+","+formatDouble4(count);
					hashtag_value = (hashtag_value.length() <= 0 ? "Hashtags WordCount: " + "\n" + value : hashtag_value + " " + value);
				}
			}
			
			
			String allvalue = key.toString().trim() + "\n" + sa_value + "\n" + wc_value + "\n" + hashtag_value + "\n";
			output.add(new StringIntPair(allvalue, topic_sum));
			Collections.sort(output,
	        new Comparator<StringIntPair>() {
	            public int compare(StringIntPair o1, StringIntPair o2) {
	                return o2.getInteger()-o1.getInteger();
	            }
	        });
			
			if (output.size() > 10)
				output.remove(output.size()-1);

		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		int i = 1;
		for (Object o:output) {
			log.info(""+o);
			System.out.println(o.toString());
			
			context.write(new Text("Top "+ i + ": \n"), new Text(""+ o));
			
			context.write(new Text(""), new Text("====================================================="));
			
			String topic = o.toString().split("\n")[0].trim();
			
			for (int j =1; j<=26; j++) {
				String value = week_output.get(topic+"_"+j);
				if (value != null && !value.isEmpty())
					context.write(new Text(topic+"_"+j), new Text(week_output.get(topic+"_"+j)));
			}
			
			i ++;
		}

	}
	
	public static String formatDouble2(double d) {
        DecimalFormat df = new DecimalFormat("0.00");

        
        return df.format(d);
    }
	
	public static String formatDouble4(double d) {
        DecimalFormat df = new DecimalFormat("0.0000");

        
        return df.format(d);
    }
}
