# CLOE4SC
CLOE4SC (Cross-lingual Ontology Enrichment for Scholarly Communication) is used for enriching an ontology, the target ontology TO, using another one, the source ontology SO, in a different natural language.

All implementations are based on Scala 2.11.11 and Apache Spark 2.3.1. 

How to use
----------
````
git clone https://github.com/shmkhaled/CLOE4SC.git
cd CLOE4SC

mvn clean package
````
Add the external libraries [java-string-similarity-1.2.0.jar](https://github.com/tdebatty/java-string-similarity/releases/download/v1.2.0/java-string-similarity-1.2.0.jar), stanford-postagger.jar and german-fast.tagger from [StanfordNLP](https://nlp.stanford.edu/software/tagger.shtml#Download) in the resource folder. 

The subsequent steps depend on your IDE. Generally, just import this repository as a Maven project and start using CLOE4SC.