#!/bin/sh
aws s3 rm s3://semantic-similarity-amit --recursive && aws s3 mv out/artifacts/Semantic_Similarity_Classification_by_various_measures_jar/EMR.jar s3://semantic-similarity-amit
