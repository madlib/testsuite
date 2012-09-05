import os, sys
import sgmllib

class MyParser(sgmllib.SGMLParser):
    "A simple parser class."

    def parse(self, s):
        "Parse the given string 's'."
        self.feed(s)
        self.close()

    def __init__(self, verbose=0):
        "Initialise an object, passing 'verbose' to the superclass."

        sgmllib.SGMLParser.__init__(self, verbose)
        self.topics = {}
        self.texts = {}
        self.id = None
        self.inside_topic = False
        self.inside_body = False

    def start_reuters(self, attributes):
        for name, value in attributes:
            if name == "newid":
                self.id = value

    def start_topics(self, attributes):
        self.inside_topic = True
    
    def end_topics(self):
        self.inside_topic = False

    def start_body(self, attributes):
        self.inside_body = True

    def end_body(self):
        self.inside_body = False

    def handle_data(self, data):
        if self.inside_topic:
            self.topics[self.id] = data

        if self.inside_body:
            if self.id in self.texts:
                self.texts[self.id] += data
            else:
                self.texts[self.id] = data

    def get_topics(self):
        return self.topics
    def get_texts(self):
        return self.texts


def unique(seq, idfun=None): 
   # order preserving
   if idfun is None:
       def idfun(x): return x
   seen = {}
   result = []
   for item in seq:
       marker = idfun(item)
       if marker in seen: continue
       seen[marker] = 1
       result.append(item)
   return result

def count_words(seq):
    dict = {}
    for word in seq:
        if word in dict:
            dict[word] += 1
        else:
            dict[word] = 1
    return dict

def get_file_words(lines):
    words = []
    for line in lines:
        word = ''
        for c in line:
            if c.isalpha():
                word += c.lower()
            else:
                if word:
                    words.append(word)
                    word = ''
    return words


def get_google_line(words, file_name, dictory = None):
    dict = count_words(words)
    out = ''
    for key, value in dict.iteritems():
        if dictory is None:
            out += key + ' ' + str(value) + ' '
        else:
            if key in dictory:
                out += key + ' ' + str(value) + ' '

    out += '\n'
    return out

def get_R_line(words, dictionary):
    dict = count_words(words)
    out = ''
    for key, value in dict.iteritems():
        index = dictionary.index(key)
        out += str(index) + ':' + str(value) + ' '
    out = str(len(out.split(':')) - 1) + ' ' + out
    out += '\n'
    return out

def get_file_list(r):
    file_path_list = []
    file_name_list = []
    for root,dirs,files in os.walk(r):
        for file in files:
            file_path_list.append(root + '/' + file)
            file_name_list.append(file)
    return file_path_list, file_name_list

def write_dict_file(dict, file):
    out = open(file, 'w')
    str = "','".join(dict)
    str = "'" + str + "'"
    out.write(str)

def get_madlib_line(words, dict):
    indexes = []
    for word in words:
        if word in dict:
            indexes.append(str(dict.index(word) + 1))
    out = ','.join(indexes)
    out += '\n'
    return out





