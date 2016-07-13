try:
    from settings import *
    from model import *
except:
    from src.settings import *
    from src.model import *
import json

with open(PRODUCTS_TRAINING_RAW_PATH) as file, open(PRODUCTS_TRAINING_PATH, 'w') as output:
    output_dict = {}
    for line_num, line in enumerate(file):
        # convert each json line into a product object
        d = json.loads(line)
        l = Product()
        l.populate(objdict(d))
        # save the standard json for a product along with the labelled_id
        cleaned_dict = l.to_dict()
        cleaned_dict['labelled_id'] = d['labelled_id']
        output_dict[line_num] = cleaned_dict
    output.write(json.dumps(output_dict))

with open(LISTINGS_TRAINING_RAW_PATH) as file, open(LISTINGS_TRAINING_PATH, 'w') as output:
    output_dict = {}
    for line_num, line in enumerate(file):
        # convert each json line into a listing object
        d = json.loads(line)
        d['words'] = d['title'].split(' ')
        l = Listing()
        l.populate(objdict(d))
        # save the standard json for a listing along with the labelled_id
        cleaned_dict = l.to_dict()
        cleaned_dict['labelled_id'] = d['labelled_id']
        output_dict[line_num] = cleaned_dict
    output.write(json.dumps(output_dict))
