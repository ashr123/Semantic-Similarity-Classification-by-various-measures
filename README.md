# Semantic Similarity Classification by various measures

## Project by Roy Ash and Amit Binenfeld

This work is a final project in the course 'Distributed System Programming: Scale Out with Cloud Computing and
Map-Reduce' by Dr. M. Adler in Ben-Gurion University \[Spring 2021\].

This work was implemneted using Java, Apache Hadoop, WEKA and Amazon EMR (AWS).

In this project, we try to reproduce the paper 'Comparing Measures of Semantic Similarity' by Nikola Ljubešić et al.
which aims at comparing different methods for automatic extraction of semantic similarity mesaures from a corpus.

We designed this project with Map-Reduce and changed some aspects of the paper by modifing the following in the
algorithm:

* **Datasets.** We used the 'English All - Biarcs' dataset of 'Google Syntactic N-Grams', instead of the 'Vjesnik'
  corpus, which provides syntactic parsing of Google-books N-Grams. Furthermore, we used our own 'golden-standard'
  dataset which composed of about 15K word pairs and their True/False indication of their relatedness. When processing
  both datasets, we used 'Porter Stemmer' in order improve the classifier model described later.
* **Methods for Building Co-occurrence Vectors.** Instead of a 'window' of words, our vector will be based on the words
  which are directly connected to the given word in the syntactic trees.
* **Features.** Instead of taking the lexemes of the context words as features, we define the features as pairs of words
  and dependency-labels, that is, the words which are connected in the syntactic tree to the given word and the label on
  the edges between them.
* **Classification.** We went one step beyond the researchers efforts and built a classifier, with WEKA api, based on
  the scores of each combination. For each pair of words in the golden-standard dataset, we defined a 24-dimensions
  vector, where each entry denote the similairity score for these two words according to one combination of association
  with context and vector similarity measures.
* **Evaluation.** We evaluate the trained model by applying 10-fold cross validation on the provied dataset. The final
  result are the Precision, Recal and F1 scores of the classifier.

## Design

### CorpusWordCount.java

This Map-Reduce job is responsible on reading the input 'Google Syntactic N-Grams' corpus, process
each `⟨word, dependency-label⟩` pair and count how often those pairs occur. In addition to that, we calculate the
values `count(f)`, `count(l)` and `count(F)` (= `count(L)`) from the paper.

Mapper input: `⟨LongWritable, Text⟩`\
Mapper output: `⟨StringStringPair, LongWritable⟩`

The mapper receives line number and text structured like that: head word, \[⟨word, pos tag, dep label, head index⟩\],
total count, counts by year. His output sends bunch of ⟨word, dep label⟩ pairs and the number of their occurences, which
for now is 1.

Reducer input: `⟨StringStringPair, LongWritable⟩`\
Reducer output: `⟨StringStringPair, LongWritable⟩`

The reducer receives the ⟨word, dep label⟩ pairs from the mapper and initiating a 'Word Count' procedure.

Memory wise both the mapper and the reducer are O(1).

### CorpusPairFilter.java

This Map-Reduce job is responsible on features filtering. We sort and select the 1000 most
common `⟨word, dependency label⟩` pairs (i.e features) and omitting the first 100. We wrote and saved them on local
HDFS.

Mapper input: `⟨StringStringPair, LongWritable⟩`\
Mapper output: `⟨LongWritable, StringStringPair⟩`

The mapper receives the ⟨word, dep label⟩ pairs and their occurences from 'CorpusWordCount' job and changes the
key,value placement in order for Hadoop with sort the output by occurences.

Reducer input: `⟨LongWritable, StringStringPair⟩`\
Reducer output: `⟨Void, Void⟩`

The reducer receives the pairs by occurences order and writes to a local file on HDFS so the reducer doesn't return
anything.

In this job the memory usage is a StringBuilder that accumulates 1000 pairs and gives them index as strings. We assume
that it doesn't take much memory.

### BuildCoVectors.java

This Map-Reduce job is responsible for building the co-occurences vectors. For every head-word that exists both on the
corpus and in the golden-standard we build 4 vectors, according to the paper's methods of association, where each vector
represents in a different way the word in the embedded space.

Mapper (Record Filter) input: `⟨LongWritable, Text⟩`\
Mapper (`count(l)`) input: `⟨StringStringPair, LongWritable⟩`\
Mappers output: `⟨Text, StringStringPair⟩`

The first mapper receives as input the 'Google Syntactic N-Grams' corpus and sends pairs of head-word and it's features
in order to calculate `count(f)` in the reducer.\
The second mapper receives the output from 'CorpusWordCount' job and sends the `count(l)` value for each word from the
corpus.

Reducer input: `⟨Text, StringStringPair⟩`\
Reducer output: `⟨Text, VectorsQuadruple⟩`

The reducer receives the output from both mappers, creates 4 vectors for each head-word and calculates 4 different
measures of association with each feature. It returns as output the head-word and it's 4 vectors.

When considering memory, for every word we create 4 arrays with 1000 cells each, each cell is of type `long` or `double`
, each of them has size of 8 bytes, that means that in any given moment we hold about `8 * 1000 * 4 = 32000` bytes (not
included JVM's overhead) in memory.\
in addition, we hold 2 more things: the 1000 features and their index produced in the former stage and `long` array of
1000 cells (8000 bytes) holding per featur's index its `count(f)`, that's not too much for any computer capable of
running Hadoop.

### BuildDistancesVectors.java

This Map-Reduce job is responsible for building the distance and similarity 24-dimension vector for each pair of words.

Mapper input: `⟨Text, VectorsQuadruple⟩`\
Mappers output: `⟨StringBooleanPair, StringVectorsQuadruplePair⟩`

The mapper receives head-word and its 4-vectors. It sends to the reducer for each head-word connected neighbor the
neighbor as key and the head-word and it's 4-vectors as value.

Reducer input: `⟨StringBooleanPair, StringVectorsQuadruplePair⟩`\
Reducer output: `⟨ArrayWritable, BooleanWritable⟩`

The reducer calculates the 24-dimension vector for each pair of words.

In the memory section, the mapper holds at any time 1 word and 4 vectors with 1000 cells each (32000 bytes) and the
reducer holds at any time 2 words and their 4-vectors (64000 bytes).\
In addition to that, we created 1 `double` array with 24 cells and 10 arrays with 4 cells each, that
means `(24 + 10 * 4) * 8 = 512` bytes.

### WEKA Classification

Training model with MLP and calculating precision, recall and F1 metrics.

## Communication

### CorpusWordCount.java

|                               | With combiners |
|-------------------------------|----------------|
| Map input records             | 267,275,914    |
| Map output records            | 1,872,999,026  |
| Map output bytes              | 43,157,067,308 |
| Combine input records         | 1,917,797,697  |
| Combine output records        | 74,339,362     |
| Map output materialized bytes | 285,726,525    |
| Reduce input records          | 29,540,691     |
| Reduce output records         | 3,797,550      |

### CorpusPairFilter.java

|                               | Without combiners |
|-------------------------------|-------------------|
| Map input records             | 3,797,550         |
| Map output records            | 2,706,127         |
| Map output bytes              | 57,004,115        |
| Map output materialized bytes | 29,438,936        |
| Reduce input records          | 1,100             |
| Reduce output records         | 0                 |

### BuildCoVectors.java

|                               | Without combiners |
|-------------------------------|-------------------|
| Map input records             | 271,073,464       |
| Map output records            | 212,334,539       |
| Map output bytes              | 3,530,176,075     |
| Map output materialized bytes | 719,614,032       |
| Reduce input records          | 212,334,539       |
| Reduce output records         | 4,953             |

### BuildDistancesVectors.java

|                               | Without combiners |
|-------------------------------|-------------------|
| Map input records             | 4,953             |
| Map output records            | 19,302            |
| Map output bytes              | 618,221,601       |
| Map output materialized bytes | 193,461,967       |
| Reduce input records          | 19,302            |
| Reduce output records         | 13,862            |

## Results

### For 15/99 parts of the corpus (that have been uploaded to S3)

Took about 32 minuts in Amazon EMR with 6 instances of type C5XLarge.

Evaluation:\
=== Summary ===

Correctly Classified Instances 2532 91.342%\
Incorrectly Classified Instances 240 8.658%\
Kappa statistic 0.1905\
K&B Relative Info Score -6.7678%\
K&B Information Score -82.9008 bits -0.0299 bits/instance\
Class complexity | order 0 1224.9355 bits 0.4419 bits/instance\
Class complexity | scheme 1208.3778 bits 0.4359 bits/instance\
Complexity improvement     (Sf)        16.5577 bits 0.006 bits/instance\
Mean absolute error 0.1513\
Root mean squared error 0.2778\
Relative absolute error 90.7567%\
Root relative squared error 96.2734%\
Total Number of Instances 2772\

Recall:                                99.24543288324067%\
Precision:                               91.875%\
F1:                                       95.41809851088201%

### For only 1 part

Took about 5 minuts in Amazon EMR with 6 instances of type C5XLarge.

Evaluation:\
=== Summary ===

Correctly Classified Instances 2477 90.9658%\
Incorrectly Classified Instances 246 9.0342%\
Kappa statistic 0.0465\
K&B Relative Info Score -12.0854%\
K&B Information Score -145.2159 bits -0.0533 bits/instance\
Class complexity | order 0 1201.5845 bits 0.4413 bits/instance\
Class complexity | scheme 1220.2651 bits 0.4481 bits/instance\
Complexity improvement     (Sf)       -18.6806 bits -0.0069 bits/instance\
Mean absolute error 0.1541\
Root mean squared error 0.2868\
Relative absolute error 92.5798%\
Root relative squared error 99.4904%\
Total Number of Instances 2723\

Recall:                                   99.83831851253031%\
Precision:                             91.07669616519174%\
F1:                                       95.25645969919012%

## Analysis

TODO: ...

## Running Instructions

TODO: ...

## Citations

* [N. Ljubesic, D. Boras, N. Bakaric and J. Njavro, "Comparing Measures of Semantic Similarity," ITI 2008 - 30th
  International Conference on Information Technology Interfaces, Cavtat, Croatia, 2008, pp. 675-682, doi:
  10.1109/ITI.2008.4588492.](Comparing-Measures-of-Semantic-Similarity.pdf)

* ![קח תהנה](Comparing-Measures-of-Semantic-Similarity.pdf)

* [Google Syntactic N-grams](http://commondatastorage.googleapis.com/books/syntactic-ngrams/index.html).
