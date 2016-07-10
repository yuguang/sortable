import settings, os, re, itertools, json
from dedupe.api import Gazetteer, StaticGazetteer
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SQLContext

LISTINGS_PATH = os.path.join(settings.INPUT_DIR, 'listings.txt')
PRODUCTS_PATH = os.path.join(settings.INPUT_DIR, 'products.txt')
OUTPUT_PATH = os.path.join(settings.OUTPUT_DIR, 'matchings.txt')
GAZETTEER_SETTINGS_PATH = 'settings'
'''
1. Filter out duplicate rows in listings
2. Train model
3. Partition listings and match with products
4. Collect together listings under the same products
'''

class GazetteerFactory:
    def getGazetteer(self):
        if os.path.exists(GAZETTEER_SETTINGS_PATH):
            with open(GAZETTEER_SETTINGS_PATH, 'rb') as f :
                return StaticGazetteer(f)
        else:
            fields = [{'field': 'name', 'type': 'String'},
                      {'field': 'manufacturer', 'type': 'String'},
                      ]
            return Gazetteer(fields)

if __name__ == "__main__":
    conf = SparkConf().setAppName("reddit")
    conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    # filter out duplicate rows in listings
    listings = sqlContext.jsonFile(LISTINGS_PATH).distinct()

    # load gazetteer
    factory = GazetteerFactory()
    gazetteer = factory.getGazetteer()
    products = sqlContext.jsonFile(PRODUCTS_PATH)
    # 1. map to name, id, and manufacturer dictionary
    # 2. map to row number, line tuples
    # 3. collect to a map to provide easy lookup for training algorithm
    listings_dict = listings.map(lambda row: {'name': ' '.join([row['product_name'], row['family'], row['model']]), 'manufacturer': row['manufacturer'], 'id': row['currency'] + str(row['price'])})\
                            .map(lambda row: (row['id'], {'name': row['name'], 'manufacturer': row['manufacturer']}))\
                            .collectAsMap()
    products_dict = products.map(lambda row: {'name': row['title'], 'manufacturer': row['manufacturer'], 'id': row['announced-date']})\
                            .map(lambda row: (row['id'], {'name': row['name'], 'manufacturer': row['manufacturer']}))\
                            .collectAsMap()
    # train model
    gazetteer.sample(listings_dict, products_dict, 10000)
    # gazetteer.markPairs(training_pairs)
    gazetteer.train()

    if not gazetteer.blocked_records:
        gazetteer.index(data_2)

    with open(GAZETTEER_SETTINGS_PATH, 'wb') as f:
        gazetteer.writeSettings(f, index=True)

    alpha = gazetteer.threshold(data_1, recall_weight=.5)


    # print candidates
    print('clustering...')
    clustered_dupes = gazetteer.match(data_1, threshold=alpha, n_matches=1)

    print('Evaluate Clustering')
    confirm_dupes = set(frozenset((data_1[pair[0]], data_2[pair[1]]))
                        for matches in clustered_dupes
                        for pair, score in matches)

    for pair in confirm_dupes:
        print(pair)

    evaluateDuplicates(confirm_dupes, duplicates_s)

    print('ran in ', time.time() - t0, 'seconds')


