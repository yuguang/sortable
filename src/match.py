try:
    from settings import *
    from model import *
except:
    from src.settings import *
    from src.model import *
from pyspark.sql.types import *
from pyspark.sql.functions import concat_ws, col, monotonicallyIncreasingId, regexp_replace
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from dedupe import Gazetteer, trainingDataLink
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SQLContext

'''
1. Filter out duplicate rows in listings
2. Train model
3. Partition listings and match with products
4. Collect together listings under the same products
'''

def canonical_format(df, Item):
    # 1. map to name, id, and manufacturer dictionary
    # 2. map to row number, line tuples
    # 3. collect to a map to provide easy lookup for training algorithm
    def to_dict(row):
        item = Item()
        item.populate(row)
        return item.to_dict()
    return df.withColumn('id', monotonicallyIncreasingId())\
             .map(lambda row: (row.id, to_dict(row))).collectAsMap()

if __name__ == "__main__":
    conf = SparkConf().setAppName("sortable")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # load gazetteer
    fields = [{'field': 'name', 'type': 'String'},
              {'field': 'manufacturer', 'type': 'String'},
              ]
    gazetteer = Gazetteer(fields)
    # read in listings from json file
    # specifying fields makes the parsing more efficient in Spark
    listing_fields = [StructField("title", StringType(), True),
                      StructField("manufacturer", StringType(), True),
                    ]
    listings = sqlContext.read.json(LISTINGS_PATH, StructType(listing_fields)).distinct()
    # break listing title into words
    tokenizer = Tokenizer(inputCol="title", outputCol="words")
    listings = tokenizer.transform(listings)
    # read in products from json file
    product_fields = [StructField("product_name", StringType(), True),
                      StructField("manufacturer", StringType(), True),
                      StructField("family", StringType(), True),
                      StructField("model", StringType(), True),
                    ]

    products = sqlContext.read.json(PRODUCTS_PATH, StructType(product_fields))\
                                .fillna({'family': ''}) # replace nulls in family fields

    products_dict = canonical_format(products, Product)
    listings_dict = canonical_format(listings, Listing)

