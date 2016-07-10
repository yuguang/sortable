import settings, os, re, itertools, json
from datetime import datetime, timedelta
from collections import deque

LISTINGS_PATH = os.path.join(settings.INPUT_DIR, 'listings.txt')
PRODUCTS_PATH = os.path.join(settings.INPUT_DIR, 'products.txt')
OUTPUT_PATH = os.path.join(settings.OUTPUT_DIR, 'matchings.txt')
'''
1. Filter out duplicate rows in listings
2. Train model
3. Partition listings and match with products
4. Collect together listings under the same products
'''


with open(LISTINGS_PATH) as listings, open(PRODUCTS_PATH) as products, open(OUTPUT_PATH, 'w') as output_file:
    