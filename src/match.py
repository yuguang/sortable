import settings, os, re, itertools, json
from pyspark.sql.functions import concat_ws, col, monotonicallyIncreasingId, regexp_replace
from dedupe import Gazetteer, trainingDataLink
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SQLContext

LISTINGS_PATH = os.path.join(settings.INPUT_DIR, 'listings.txt')
PRODUCTS_PATH = os.path.join(settings.INPUT_DIR, 'products.txt')
LISTINGS_TRAINING_PATH = os.path.join(settings.TRAINING_DIR, 'listings.txt')
PRODUCTS_TRAINING_PATH = os.path.join(settings.TRAINING_DIR, 'products.txt')
OUTPUT_PATH = os.path.join(settings.OUTPUT_DIR, 'matchings.txt')

'''
1. Filter out duplicate rows in listings
2. Train model
3. Partition listings and match with products
4. Collect together listings under the same products
'''

def canonical_format(df, training):
    # 1. map to name, id, and manufacturer dictionary
    # 2. map to row number, line tuples
    # 3. collect to a map to provide easy lookup for training algorithm
    if not training:
        df = df.withColumn('id', monotonicallyIncreasingId())\
                .select(col('id'), df.name, df.manufacturer)
    else:
        df = df.withColumnRenamed('labelled_id', 'id')\
                .select(col('id'), df.name, df.manufacturer)

    return df.map(lambda row: (row.id, {'name': row.name, 'manufacturer': row.manufacturer})).collectAsMap()

def load_products(path, training=False):
    products = sqlContext.read.json(path)
    return canonical_format(products, training)

def load_listings(path, training=False):
    # filter out duplicate rows in listings
    listings = sqlContext.read.json(path).distinct()
    listings = listings.withColumnRenamed('title', 'name')
    return canonical_format(listings, training)

if __name__ == "__main__":
    conf = SparkConf().setAppName("reddit")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # load gazetteer
    fields = [{'field': 'name', 'type': 'String'},
              {'field': 'manufacturer', 'type': 'String'},
              ]
    gazetteer = Gazetteer(fields)
    products_dict = load_products(PRODUCTS_PATH)
    listings_dict = load_listings(LISTINGS_PATH)
    products_training_dict = load_products(PRODUCTS_TRAINING_PATH, True)
    listings_training_dict = load_listings(LISTINGS_TRAINING_PATH, True)

    # train model
    gazetteer.sample(products_dict, listings_dict, 10000)
    training_pairs = trainingDataLink(products_training_dict, listings_training_dict, 'id')
    gazetteer.markPairs(training_pairs)
    gazetteer.train()

    # partition listings and match with products
    if not gazetteer.blocked_records:
        gazetteer.index(listings_dict)

    alpha = gazetteer.threshold(products_dict, recall_weight=.5)


    # print candidates
    print('clustering...')
    clustered_dupes = gazetteer.match(products_dict, threshold=alpha, n_matches=1)

    print('Evaluate Clustering')
    confirm_dupes = set(frozenset((listings_dict[pair[0]], products_dict[pair[1]]))
                        for matches in clustered_dupes
                        for pair, score in matches)

    for pair in confirm_dupes:
        print(pair)



