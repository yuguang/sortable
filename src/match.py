from settings import *
from model import *
from collections import defaultdict
from pprint import pprint
from pyspark.sql.types import *
from pyspark.sql.functions import concat_ws, col, monotonicallyIncreasingId, regexp_replace
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from dedupe import Gazetteer, trainingDataLink
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SQLContext
import json

'''
1. Filter out duplicate rows in listings
2. Train model
3. Partition listings and match with products
4. Collect together listings under the same products
'''

def debug(obj):
    # convenience function for debug output
    if DEBUG:
        pprint(obj)

def canonical_format(df, Item):
    # 1. map to name, id, and manufacturer dictionary
    # 2. map to row number, line tuples
    # 3. collect to a map to provide easy lookup for training algorithm
    def to_dict(row):
        item = Item()
        item.populate(row)
        return item.to_dict()
    df = df.withColumn('id', monotonicallyIncreasingId())
    return (df, df.map(lambda row: (row.id, to_dict(row))).collectAsMap())

if __name__ == "__main__":
    conf = SparkConf().setAppName("sortable")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # load gazetteer
    fields = [{'field': 'name', 'type': 'String'},
              {'field': 'manufacturer', 'type': 'String'},
              {'field': 'model', 'type': 'String'},
              {'field': 'family', 'type': 'String'},
              ]
    gazetteer = Gazetteer(fields)
    # read in listings from json file
    # specifying fields makes the parsing more efficient in Spark
    listing_fields = [StructField("title", StringType(), True),
                      StructField("manufacturer", StringType(), True),
                      StructField("currency", StringType(), True),
                      StructField("price", StringType(), True),
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

    products_df, products_dict = canonical_format(products, Product)
    listings_df, listings_dict = canonical_format(listings, Listing)

    products_training_dict = json.load(open(PRODUCTS_TRAINING_PATH))
    listings_training_dict = json.load(open(LISTINGS_TRAINING_PATH))
    # train model
    gazetteer.sample(products_dict, listings_dict, 10000)
    training_pairs = trainingDataLink(products_training_dict, listings_training_dict, 'labelled_id', 10)
    gazetteer.markPairs(training_pairs)
    gazetteer.train()

    # add products to the index of records to match against
    if not gazetteer.blocked_records:
        gazetteer.index(products_dict)

    alpha = gazetteer.threshold(listings_dict, recall_weight=.5)


    # identify records that all refer to the same entity
    print('clustering...')
    clustered_dupes = gazetteer.match(listings_dict, threshold=alpha)
    debug(clustered_dupes)

    match_dict = defaultdict(list)
    # fill in null entries
    listings_df.fillna('')
    pairs = []
    # convert to data frame
    for matches in clustered_dupes:
        for pair, score in matches:
            pairs.append((int(pair[0]), int(pair[1])))
    matches_df = sqlContext.createDataFrame(pairs, ['listing_id', 'product_id'])
    # get product and listing information for each matched pair
    matches_df = matches_df.join(products_df.select('id', 'product_name'), products_df.id == matches_df.product_id).join(listings_df.select('id', 'title', 'manufacturer', 'currency', 'price'), listings_df.id == matches_df.listing_id)
    # collect listings into lists with matched products
    for match in matches_df.collect():
        match_dict[match.product_name].append({
            "title": match.title,
            "manufacturer": match.manufacturer,
            "currency": match.currency,
            "price": match.price
        })
    debug(match_dict)

    # write to a result file
    with open(OUTPUT_PATH, 'w') as output_file:
        for product_name, listings in match_dict.items():
            result = {
                "product_name": product_name,
                "listings": listings
            }
            output_file.write('{}\n'.format(json.dumps(result)))

