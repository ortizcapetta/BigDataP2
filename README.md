# Big Data Analytics Project 2
Second project for Big Data Analytics course CIIC5995 @ UPRM.
The project consists of extracting data from tweets and analyzing them based on:
1. Top keywords(trump,flu,zika,diarrhea,ebola,headache,measles)
2. Top 10 Hashtags per hour
3. Top 10 trending words per hour
4. Top 10 participants per hour

## Tweet Extraction
To obtain different data, you can run the tweet extractor with
```
python3 tweet_reader.py
```
which will run for 24 hours extracting information from live tweets.

## Tweet Analysis
Once tweets have been extracted from twitter, these can be analyzed with pyspark to obtain the desired data with:
```
python3 spark_processing.py
```
We have provided our results here, but for it to work you must not have an existing results folder. 
Our results can be found in the [results tab](/Results). 

## Results 

- In [results/hashtags](/results/hashtags) we can see many folders called hashtag#NUMBER where #NUMBER  is the hour
of runtime. Each of these contains a csv file with the top 15 hashtags per hour.
- In [results/keywords](/results/keywords) we can find the top 15 keywords per hour in folders called
keywords#HOUR as well as the full text for each of these keywords in folders called keywordsfull#HOUR. There's a
special folder here called [keywords_alltext](/results/keywords/keywords_alltext) which contains
data utilized for [Project3](https://github.com/ortizcapetta/BigDataP3) of the course.
- In [results/users](/results/users) we find the top 15 users per hour using the same format as above.
- In [results/words](/results/words) we find the top 15 trending words per hour using the same format as above. 


## Visualization
Visualizations have been provided in the [Visualization tab](/Visualization). Provided are three HTML files
created in google charts featuring all time data from our obtained results.


