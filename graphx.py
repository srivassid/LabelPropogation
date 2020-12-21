from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import SparkContext
import hashlib
from graphframes import GraphFrame
from pyspark.sql.functions import desc

class LPA():

    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName('Example_2') \
            .getOrCreate()

    def graphx(self):
        self.df = self.spark.read.option("header", "true").csv('results_new/data-00000-of-00010.csv')
        # print(self.df.show(n=5))

        self.df = self.df.dropna()
        self.rdd = self.df.select("url","mention").rdd.flatMap(lambda x: x).distinct()
        # print(self.rdd.take(5))

        def hashnode(x):
            return hashlib.sha1(x.encode("UTF-8")).hexdigest()[:8]

        hashnode_udf = udf(hashnode)

        vertices = self.rdd.map(lambda x: (hashnode(x), x)).toDF(["id", "url"])

        vertices.show(5)

        edges = self.df.select("url", "mention") \
            .withColumn("src", hashnode_udf("url")) \
            .withColumn("dst", hashnode_udf("mention")) \
            .select("src", "dst")

        edges.show(5)

        self.graph = GraphFrame(vertices, edges)
        # print(self.graph)
        print('communities are ')
        self.communities = self.graph.labelPropagation(maxIter=2)

        print(self.communities.persist().show(10))
        print(self.communities.sort(desc("label")).show(50))
        self.communities.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("communities")
        print("There are " + str(self.communities.select('label').distinct().count()) + " communities in sample graph.")

        print(self.graph.inDegrees.join(vertices, on="id") \
            .orderBy("inDegree", ascending=False).show(10))

        print(self.graph.stronglyConnectedComponents(maxIter=2).select('url','component').show(20))

if __name__ == '__main__':
    lpa = LPA()
    lpa.graphx()