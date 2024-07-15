from pyspark import SparkContext, SparkConf
import sys
import re
from itertools import combinations # https://docs.python.org/3/library/itertools.html#module-itertools

class Project2:           

    def run(self, inputPath, outputPath, stopwords, k):
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)
        
        # Broadcast stopwords
        swlist = sc.broadcast(set(sc.textFile(stopwords).collect()))
        
        # Read and parse the input data
        fp = sc.textFile(inputPath)
        category_headline = fp.map(lambda line: line.split(",", 1))  # Split into category and headline
        
        # Define a function to clean and split the headline into valid words
        def get_valid_words(line):
            words = re.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+", line.lower())
            return [word for word in words if word.isalpha() and word not in swlist.value]
        
        # Filter valid and invalid headlines
        valid_headlines = category_headline.map(lambda x: (x[0], get_valid_words(x[1]))).cache()
        invalid_headlines = valid_headlines.filter(lambda x: len(x[1]) < 3).map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
        valid_headlines = valid_headlines.filter(lambda x: len(x[1]) >= 3)
        
        # Generate term triads
        def generate_triad(line):
            category, words = line
            triads = combinations(sorted(words), 3)
            return [(category, triad) for triad in triads]
        
        triads = valid_headlines.flatMap(generate_triad)
        triad_counts = triads.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
        
        valid_headline_counts = valid_headlines.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

        relative_frequencies = triad_counts.map(lambda x: (x[0][0], (x[0][1], x[1]))).join(valid_headline_counts).map(lambda x: (x[0], x[1][0][0], x[1][0][1] / x[1][1]))
        
        results = relative_frequencies.groupBy(lambda x: x[0]).flatMap(lambda x: sorted(x[1], key=lambda y: (-y[2], y[1]))).collect()
        
        # Prepare the final output format
        output = []
        invalid_lines = invalid_headlines.collectAsMap()
        categories = sorted(set(invalid_lines.keys()).union(set(x[0] for x in results)))
        
        for category in categories:
            if category in invalid_lines:
                output.append(f"{category}\tinvalid line:{invalid_lines[category]}")
            if category in set(x[0] for x in results):
                category_results = [f"{cat}\t{','.join(triad)}:{rf}" for (cat, triad, rf) in results if cat == category]
                output.extend(category_results[:k])
       
        sc.parallelize(output).saveAsTextFile(outputPath)
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))

