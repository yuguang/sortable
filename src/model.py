import abc, re, unittest, json
from settings import *

class AbstractProduct(metaclass=abc.ABCMeta):
    delchars = ''.join(c for c in map(chr, range(256)) if ( (not c.isalnum()) and (c not in ('.')) ) )
    translate_table = dict((ord(char), None) for char in delchars)
    translate_table_space = dict((ord(char), None) for char in delchars)

    def __init__(self):
        self.manufacturer = ""
        self.model = ""
        self.family = ""
        self.name = ""

    def get_manufacturer(self):
        
        return self.manufacturer
        
    def get_model(self):
        
        return self.model
        
    def get_family(self):
        
        return self.family

    def get_name(self):

        return self._replace_non_alpha_characters_with_space(self.name)

    def to_dict(self):

        return {'manufacturer': self.get_manufacturer(), 'model': self.get_model(), 'family': self.get_family()}

    @abc.abstractmethod
    def populate(self, row):
        """Populates the manufacturer, model, and family fields
        from a DataFrame Row.
        Implemented by concrete classes"""

    def _remove_non_alpha_characters(self, string):
        """This method removes all non-alphanumeric characters.
        This implementation assumes Python 3.x unicode strings as input.
        """
        return string.translate(self.translate_table)

    def _replace_non_alpha_characters_with_space(self, string):
        """This method replaces all non-alphanumeric characters.
        This implementation assumes Python 3.x unicode strings as input.
        """
        return string.translate(self.translate_table_space)

    def __str__(self):
        header = "================{}=================".format(self.__class__.__name__)
        return '{}\nName: {}\nManufacturer: {}\nFamily: {}\nModel: {}'.format(header, self.get_name(), self.get_manufacturer(), self.get_family(), self.get_model())

class Product(AbstractProduct):

    def populate(self, row):
        self.manufacturer = row.manufacturer
        self.model = row.model
        if 'family' in row:
            self.family = row.family
        self.name = row.product_name


class Listing(AbstractProduct):

    def set_processor(self, processor):
        self.processor = processor

    def populate(self, listing):
        self.name = listing.title
        if self.processor._is_model(listing.words[1]):
            self.model = listing.words[1]
        elif self.processor._is_family(listing.words[1]):
            self.family = listing.words[1]
            if self.processor._is_model(listing.words[2]):
                self.model = listing.words[2]

class ListingTitleProcessor():
    def __init__(self):
        super().__init__()
        self.models = set()
        self.families = set()

    def set_models(self, models):
        """
        Lower cases model names and makes a set
        :param models: a list of models as strings
        """
        models = [x.lower() for x in models]
        self.models = set(models)

    def set_families(self, families):
        """
        Lower cases model names and makes a set
        :param families: a list of models as strings
        """
        families = [x.lower() for x in families]
        self.families = set(families)

    def _contains_digit(self, word):
        return bool(re.search(r'\d', word))

    def _is_model(self, word):
        return self._contains_digit(word) and word.lower() in self.models

    def _is_family(self, word):
        return not self._contains_digit(word) and word.lower() in self.families

def load_training_file(path):
    """
    Spark has high overhead for running tests due to starting and stopping the JVM.
    Therefore, I am loading the JSON files directly in a Python library
    """
    data = []
    with open(path) as f:
        for line in f:
            data.append(json.loads(line))
    return data

class objdict(dict):
    """
    Access Python dictionary items as object attributes
    """
    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise AttributeError("No such attribute: " + name)

class TestProduct(unittest.TestCase):
    def test_print(self):
        for row in load_training_file(PRODUCTS_PATH):
            p = Product()
            p.populate(objdict(row))
            print(p)

if __name__ == '__main__':
    unittest.main()