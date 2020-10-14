## Big Data Analysis for Automatic Sentiment Analysis of Social Network Hashtags
Hello there!\
This repository holds my essay: *Big Data Analysis for Automatic Sentiment Analysis of Social Network Hashtags code

This study combines two emotional lexicons, NRC Emotion Lexicon and NRC Emotion Intensity Lexicon, for sentiment analysis. Data collected the tweets include \#stayhome, \#covid19, \#coronavirus, \#blacklivesmatter, and \#alllivesmatter through Twitter API, all of them are the most popular hashtags in the first half of 2020.

This study proposes a MapReduce architecture for twitter sentiment analysis that can handle the following three situations. Tweets of multiple events can be processed at the same time for sentiment analysis, the sentiment change of each event according to the timeline, and the degree of relevance of other related events of the hashtag can be analyzed. 

## Project Source Codes:
* Final Code by MapReduce:
  * A_Final_MainClass.java (https://github.com/tzuling/Essay_Code/blob/main/src/A_Final_MainClass.java)
  * A_Final_MapClass.java (https://github.com/tzuling/Essay_Code/blob/main/src/A_Final_MapClass.java)
  * A_Final_ReduceClass.java (https://github.com/tzuling/Essay_Code/blob/main/src/A_Final_ReduceClass.java)
* Package by myselves:
  * StringIntPair.java (https://github.com/tzuling/Essay_Code/tree/main/src/StringIntPair.java)
  * Global.java (https://github.com/tzuling/Essay_Code/tree/main/src/Global.java)
* Test Code:
  * Final_MainClass.java (https://github.com/tzuling/Essay_Code/tree/main/src/Final_MainClass.java)
  * Final_MapClassbyWord.java (https://github.com/tzuling/Essay_Code/tree/main/src/Final_MapClassbyWord.java)
  * Final_ReduceClass.java (https://github.com/tzuling/Essay_Code/tree/main/src/Final_ReduceClass.java)
  * MapClassbyRegex.java (https://github.com/tzuling/Essay_Code/tree/main/src/MapClassbyRegex.java)
