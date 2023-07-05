# 5550_Final_Project : search team

#############################################################################################

Run crawler:
- Step 1: export crawler as JAR file
- Step 2: Run the following in order:
	* cis5550.kvs.Master 8000
	* cis5550.kvs.Worker 8001 localhost:8000 
	* cis5550.flame.FlameMaster 9000 localhost:8000 
	* cis5550.flame.FlameWorker 9001 localhost:9000 
- Step 3: Add seed URL in Crawler.java run configuration and run it

######################################################################################
Run indexer and searcher:
- approved third-party components: lib/json-simple-1.1.1.jar, lib/jsoup-1.15.3.jar
- Indexer: 
  Compliling the code:
  javac -cp lib/json-simple-1.1.1.jar:lib/jsoup-1.15.3.jar --source-path src src/cis5550/jobs/Indexer.java
  Run the code:
  java -cp lib/jsoup-1.15.3.jar:src cis5550.jobs.Indexer crawl localhost:8000
  
- Searcher:
  Compliling the code:
  javac -cp lib/json-simple-1.1.1.jar:lib/jsoup-1.15.3.jar --source-path src src/cis5550/search/Searcher.java
  Run the code:
  java -cp lib/json-simple-1.1.1.jar:src cis5550.search.Searcher 8888 localhost:8000
######################################################################################

Run front-end:
- npm install
- npm start

use port 3000
seach page url: http://searchteam.cis5550.net:3000/
######################################################################################
