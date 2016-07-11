import abc, re, unittest, json
from settings import *

class AbstractProduct(metaclass=abc.ABCMeta):
    delchars = ''.join(c for c in map(chr, range(256)) if ( (not c.isalnum()) and (c not in ('.')) ) )
    translate_table = dict((ord(char), None) for char in delchars)
    translate_table_space = dict((ord(char), ' ') for char in delchars)

    def __init__(self):
        self.manufacturer = ""
        self.model = ""
        self.family = ""
        self.name = ""

    def get_manufacturer(self):
        
        return self._remove_non_ascii_characters(self.manufacturer)
        
    def get_model(self):
        
        return self._remove_non_ascii_characters(self.model)
        
    def get_family(self):
        
        return self._remove_non_ascii_characters(self.family)

    def get_name(self):
        name = self._replace_non_alpha_characters_with_space(self.name)
        return re.sub(r'\s+', ' ', name)

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

    def _remove_non_ascii_characters(self, text):
        return ''.join([i if ord(i) < 128 else '' for i in text])

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

    def _clean_original_title(self, title):
        title = self._remove_non_ascii_characters(title)
        model_desc = title.replace(self.get_manufacturer(), ' ').replace(self.get_family(), ' ')
        return model_desc.strip(self.delchars)

    def _contains_digit(self, word):
        return bool(re.search(r'\d', word))

    def _is_model(self, word):
        return self._contains_digit(word)

    def _is_family(self, word):
        return not self._contains_digit(word) and not word == '-'

    def _populate_non_matching(self, listing):
        if not len(listing.words):
            return
        for word in listing.words[1:]:
            if self._is_model(word):
                self.model = word
                break
            elif self._is_family(word):
                self.family = word

    def populate(self, listing):
        self.name = listing.title
        # set manufacturer and family
        pattern = r'([a-zA-Z]+)\s([a-zA-Z\-]+)\s.*'
        if re.match(pattern, self.name):
            self.manufacturer, self.family = re.match(pattern, self.name).groups()
        elif re.match(r'([a-zA-Z]+)\s.*', self.name):
            self.manufacturer, = re.match(r'([a-zA-Z]+)\s.*', self.name).groups()
        else:
            self.manufacturer = listing.manufacturer
        # remove manufacturer and model from original title
        model_desc = self._clean_original_title(listing.title)
        # models are usually all caps with numbers and optional dashes
        for pattern in [r'([A-Z]+\-[A-Z\d]+).*', r'([A-Z]+\s+[A-Z\d]+).*', r'([A-Z\d]+).*', r'([\d]+\s+[A-Za-z]+).*', r'([A-Za-z\d\-]+).*']:
            matches = re.match(pattern, model_desc)
            if matches:
                self.model = matches.groups()[0]
                break
        if len(self.family) < 2 and len(self.model) < 2:
            # use alternative method to populate these fields
            self._populate_non_matching(listing)

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
            self.assertTrue(isinstance(str(p), str))
            # print(p)

    def test_manufacturer(self):
        p = Product()
        p.populate(objdict({"product_name":"Olympus_C-21","manufacturer":"Olympus","model":"C-21","announced-date":"1999-06-27T20:00:00.000-04:00"}))
        self.assertEqual('Olympus', p.get_manufacturer())

    def test_model(self):
        p = Product()
        p.populate(objdict({"product_name":"Olympus_C-21","manufacturer":"Olympus","model":"C-21","announced-date":"1999-06-27T20:00:00.000-04:00"}))
        self.assertEqual('C-21', p.get_model())

class TestListing(unittest.TestCase):
    def test_clean(self):
        l = Listing()
        l.manufacturer = 'Panasonic'
        l.family = 'Lumix'
        self.assertEqual('DMC-G1K Kit Standard', l._clean_original_title('Panasonic - Lumix DMC-G1K Kit Standard'))
        l = Listing()
        l.manufacturer = 'Olympus'
        l.family = 'PEN'
        self.assertEqual('E-P1 Kit - Compact  objectifs interchangeables', l._clean_original_title('Olympus - PEN E-P1 Kit - Compact à objectifs interchangeables'))
        l = Listing()
        l.manufacturer = 'Ricoh'
        l.family = 'Unit'
        self.assertEqual('P10 28-300 mm pour appareil photo GXR', l._clean_original_title('Ricoh - UnitÃ© P10 28-300 mm pour appareil photo GXR'))

    def test_family(self):
        l = Listing()
        l.populate(objdict({"title":"Canon PowerShot ELPH 300 HS 12 MP CMOS Digital Camera with Full 1080p HD Video (Black)","manufacturer":"Canon","currency":"USD","price":"229.99"}))
        self.assertEqual(l.get_family(), 'PowerShot')

    def test_model(self):
        l = Listing()
        l.populate(objdict({"title":"Canon PowerShot ELPH 300 HS 12 MP CMOS Digital Camera with Full 1080p HD Video (Black)","manufacturer":"Canon","currency":"USD","price":"229.99"}))
        self.assertEqual('ELPH 300', l.get_model())
        l = Listing()
        l.populate(objdict({"title":"Sony Cyber-Shot DSC-TX10 16.2 MP Waterproof Digital Still Camera with Exmor R CMOS Sensor, 3D Sweep Panorama and Full HD 1080/60i Video (Black)","manufacturer":"Sony","currency":"USD","price":"329.99"}))
        self.assertEqual('Cyber-Shot', l.get_family())
        self.assertEqual('DSC-TX10', l.get_model())

    def test_model_alt(self):
        l = Listing()
        row = {"title":"Ricoh - UnitÃ© P10 28-300 mm pour appareil photo GXR","manufacturer":"RICOH","currency":"EUR","price":"249.00"}
        row['words'] = row['title'].split(' ')
        l.populate(objdict(row))
        self.assertEqual('Unit', l.get_family())
        self.assertEqual('P10', l.get_model())

    def test_remove_characters(self):
        l = Listing()
        test_string = 'Ricoh - UnitÃ© P10 28-300 mm pour appareil photo GXR'
        self.assertEqual('Ricoh - Unit P10 28-300 mm pour appareil photo GXR', l._remove_non_ascii_characters(test_string))

if __name__ == '__main__':
    unittest.main()