from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import re

class Project2:           
    def run(self, inputPath, outputPath, stopwords, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        
        # Fill in your code here
        swlist = spark.sparkContext.broadcast(set(spark.sparkContext.textFile(stopwords).collect()))

        fp = spark.sparkContext.textFile(inputPath)
        category_headline = fp.map(lambda line: line.split(",", 1))

        def get_valid_words(line):
            words = re.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+", line.lower())
            return [word for word in words if word.isalpha() and word not in swlist.value]

        # Convert RDD to DataFrame
        headlines_df = category_headline.toDF(["category", "headline"])

        # Handle invalid headlines
        invalid_lines = headlines_df.withColumn("valid_words", udf(get_valid_words, ArrayType(StringType()))("headline")).filter(size(col("valid_words")) < 3).groupBy("category").agg(count("*").alias("invalid_count")).collect()

        # invalid headlines 
        valid_headlines_df = headlines_df.withColumn("valid_words", udf(get_valid_words, ArrayType(StringType()))("headline")).filter(size(col("valid_words")) >= 3)

        # Define UDF to generate triads and sort them alphabetically
        def generate_triad_udf(words):
            triads = []
            n = len(words)
            for i in range(n):
                for j in range(i + 1, n):
                    for k in range(j + 1, n):
                        triad = sorted([words[i], words[j], words[k]])
                        triads.append((triad[0], triad[1], triad[2]))
            return triads

        # Register UDF for generating triads
        spark.udf.register("generate_triad_udf", generate_triad_udf, ArrayType(ArrayType(StringType())))

        # Apply UDF to generate triads and explode resulting array into rows
        triads_df = valid_headlines_df.withColumn("triad", explode(expr("generate_triad_udf(valid_words)"))).select("category", "triad")

        # Count the number of valid headlines per category
        valid_headline_counts_df = valid_headlines_df.groupBy("category").agg(count("*").alias("valid_count"))

        # Count occurrences of each triad
        triad_counts_df = triads_df.groupBy("category", "triad").agg(count("*").alias("triad_count"))

        # Compute relative frequency of each term triad
        relative_frequencies_df = triad_counts_df.join(valid_headline_counts_df, "category").withColumn("relative_frequency", col("triad_count") / col("valid_count")).orderBy(col("category"), col("relative_frequency").desc(), col("triad"))

        # Format and collect results
        results_df = relative_frequencies_df.selectExpr("category", "concat_ws(',', triad) as triad", "relative_frequency").orderBy(col("category"), col("relative_frequency").desc(), col("triad"))

        # Collect results and prepare final output
        results = results_df.collect()
        output = []

        categories = sorted(set(row["category"] for row in invalid_lines).union(set(row["category"] for row in results)))

        for category in categories:
            invalid_count = next((row["invalid_count"] for row in invalid_lines if row["category"] == category), 0)
            output.append(f"{category}\tinvalid line:{invalid_count}")

            category_results = [f"{row['category']}\t{row['triad']}:{row['relative_frequency']}" for row in results if row['category'] == category]
            output.extend(category_results[:k])

        # output
        output_rdd = spark.sparkContext.parallelize(output)
        output_rdd.coalesce(1).saveAsTextFile(outputPath)
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))


