import os
PROJECT_DIR = os.path.dirname(os.path.dirname(__file__))
INPUT_DIR = os.path.join(PROJECT_DIR, 'data_input')
OUTPUT_DIR = os.path.join(PROJECT_DIR, 'data_output')
TRAINING_DIR = os.path.join(PROJECT_DIR, 'data_training')

LISTINGS_PATH = os.path.join(INPUT_DIR, 'listings.txt')
PRODUCTS_PATH = os.path.join(INPUT_DIR, 'products.txt')
LISTINGS_TRAINING_RAW_PATH = os.path.join(TRAINING_DIR, 'listings.txt')
PRODUCTS_TRAINING_RAW_PATH = os.path.join(TRAINING_DIR, 'products.txt')
LISTINGS_TRAINING_PATH = os.path.join(TRAINING_DIR, 'listings.json')
PRODUCTS_TRAINING_PATH = os.path.join(TRAINING_DIR, 'products.json')
OUTPUT_PATH = os.path.join(OUTPUT_DIR, 'matchings.txt')
DEBUG = False