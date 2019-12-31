import glob
import os
import string

import nltk
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.stem.porter import *
import collections


DOCUMENT_PARSE_KEY = "reuters"


POSTING_ATTRIBUTE = "newid"

BLOCK_FILE_PATH_REGEX = "./AiWebBlocks/*.txt"
INDEX_FILE_PATH_TEMPLATE = "./AiWebIndex/index{}.txt"
BLOCK_FILE_PATH_TEMPLATE = "./AiWebBlocks/block{}.txt"

INDEX_FILE_SIZE = 2500000
MEMORY_CAPACITY = 99999999999999999

'''
parse a single file to return a list of documents at the level of PARSE_KEY
'''

ROOT = "C:\\Users\\Vera Zhao\\Desktop\\aiWeb\\aiWeb\\aitopics.org"

# only one html in a file
def parse_file(file_directory):
    f = open(file_directory, "r", encoding="iso8859_2")
    all_file_with_style = f.read()
    f.close()
    # build the document tree by BeautifulSoup
    DOM = BeautifulSoup(all_file_with_style, features="html.parser")
    # remove all the tag that don't used
    [x.extract() for x in DOM.findAll('script')]
    [x.extract() for x in DOM.findAll('style')]
    [x.extract() for x in DOM.findAll('noscript')]
    [x.extract() for x in DOM.findAll('audio')]
    [x.extract() for x in DOM.findAll('iframe')]
    try:
        # if the html is match the requirement
        html = DOM.findAll("html")[0].body.text
        return html  # string
    except IndexError:
       # print("tag error")
        return None

def generate_tokens_pipeline(text):
    tokens = nltk.word_tokenize(text)
    tokens = list(filter(lambda token: token not in string.punctuation, tokens))
    tokens = list(
        filter(lambda token: len(re.findall(r"^\d+(\.|,|\/|\-|\d+)*$", token)) == 0, tokens))  # ^\d+(\.|,|\/|\-|\d+)*$
    tokens = [token.lower() for token in tokens]

    nltk_words = list(stopwords.words('english'))
    tokens = [token for token in tokens if token not in nltk_words]

    # stemmer = PorterStemmer()
    # tokens = [stemmer.stem(token) for token in tokens]
    #
    # wnl = nltk.WordNetLemmatizer()
    # tokens = [wnl.lemmatize(t) for t in tokens]

    tokens = [t for t in tokens if t != ' ']
    return tokens

# for each url/file is only one document
def clean_source(url, html_text, d_length):
    single_doc = []
    if url is not None:
        single_doc.append(url[18:])
    else:
        single_doc.append(None)
    if html_text is not None:
        tokens = generate_tokens_pipeline(html_text)
        d_length += len(tokens)
        single_doc.append(tokens)

    else:
        single_doc.append("")
    return single_doc, d_length #single_doc [url, [token1, token2.....]]

# spimi
def build_inverted_index_in_memory(inverted_index, single_doc): #[url, [token1,token2...]
    counter = collections.Counter(single_doc[1])
    for token in single_doc[1]:
        if inverted_index.get(token, None) is not None:
            inverted_index[token].add("~".join([str(single_doc[0]), str(len(single_doc[1])), str(counter[token])])) # (id,body)->(url, doc length, term count/term_frency)
        else:
            inverted_index[token] = set(["~".join([str(single_doc[0]), str(len(single_doc[1])), str(counter[token])])])  # string



def persist_memory_data(inverted_index, f_name):
    f = open(f_name, "w")
    for key in sorted(inverted_index.keys()):
        try:
            f.write(key + "`" + ":-)".join(sorted(inverted_index.get(key), key=lambda s: s.split("~")[0])) + "\n")
        except Exception:
            continue
    f.close()


def read_line_from_block(block_file_obj, block_number):
    top_line = block_file_obj.readline()
    key_values_pair = top_line.rstrip("\n").split("`")
    return [key_values_pair[0], [block_number, key_values_pair[1].split(":-)")]]


def merge_blocks(block_files):
    global ending_words
    f_df = open("./AiFileDF.txt", "w")
#    def sorted_as_int(nums):
#        nums = [num for num in nums if len(re.findall(r"\d+", num.rstrip("\n"))) > 0]
#        nums = sorted(nums, key=lambda s: int(s.split("~")[0]))
#        return nums

    non_positional_postings_size = 0

    output_file_name = INDEX_FILE_PATH_TEMPLATE
    output_file_count = 0
    f = open(output_file_name.format(output_file_count), "w")
    count = 0

    files = [i for i in range(len(block_files))]  # [0, 1, 2, ..., 42]
    for file_name in block_files:
        index = int(re.findall(r"\d+", file_name)[0])
        files[index] = open(file_name, "r")

    lines = {}
    for i in range(len(files)):
        try:
            line = read_line_from_block(files[i], i)
        except IndexError:
            continue
        if line == "":
            files[i].close()
        else:
            if lines.get(line[0], None) is not None:
                lines[line[0]].append(line[1])
            else:
                lines[line[0]] = [line[1]]

    while len(lines.keys()) > 0:  # [ key, [index, [value1, value2]] ]
        token = sorted(lines.keys())[0]
        postings = [value[1] for value in lines.get(token)]
        index_lst = [value[0] for value in lines.get(token)]

        p = []
        for posting in postings:
            p.extend(posting)

        # p = sorted_as_int(list(set(p)))
        non_positional_postings_size += len(p)
        count += 1
        f.write(str(token) + "`" + ":-)".join(p) + "\n")
        f_df.write(str(token) + " = " + str(len(p)) + "\n")
        if count == INDEX_FILE_SIZE:
            ending_words.append(token)
            f.close()
            output_file_count += 1
            f = open(output_file_name.format(output_file_count), "w")
            count = 0
        del lines[token]

        for index in index_lst:
            try:
                line = read_line_from_block(files[index], index)
            except IndexError:
                continue
            if line == "":
                files[index].close()
            else:
                if lines.get(line[0], None) is not None:
                    lines[line[0]].append(line[1])
                else:
                    lines[line[0]] = [line[1]]
    f.close()
    f_df.close()
    return int(output_file_count * INDEX_FILE_SIZE + count), non_positional_postings_size



if __name__ == "__main__":
    inverted_index_dictionary = {}
    ordered_top = {}
    block_number = 0
    d_length = 0

# join the path and the name, find all the files
    files = [os.path.join(path, name) for path, subdirs, files in os.walk(ROOT) for name in files][0:3000]
    print(files)
    print("[INFO] SPIMI generating block files begins")
    counter = 0
    for file in files:
        # return single html page (extract all useless info
        doc = parse_file(file)
        # make sure that all the file is in html file
        # or it is null or other exceptions, not a doc
        if file.find("html") < 0 or not doc:
            continue
        # each file is url
        cleaned_doc, d_length = clean_source(file, doc, d_length)

        # print(d_length)
        build_inverted_index_in_memory(inverted_index_dictionary, cleaned_doc)
        counter += 1
        if counter == MEMORY_CAPACITY:
            counter = 0
            persist_memory_data(inverted_index_dictionary, BLOCK_FILE_PATH_TEMPLATE.format(str(block_number)))
            inverted_index_dictionary = {}
            block_number += 1
    persist_memory_data(inverted_index_dictionary, BLOCK_FILE_PATH_TEMPLATE.format(str(block_number)))

    print("[INFO] SPIMI generating block files ends")
    files = glob.glob(BLOCK_FILE_PATH_REGEX)
    print("[INFO] Merging blocks begins")
    ending_words = []
    distinct_term_size, non_positional_postings_size = merge_blocks(files)
    doc_number = block_number*MEMORY_CAPACITY + counter
    lave = round(d_length/doc_number)
    print("[INFO] Total number of distinct_term is: ", distinct_term_size)
    print("[INFO] Total number of non_positional_postings is: ", non_positional_postings_size)
    print("[INFO] Ending words for each index file: ", ending_words)
    print("[INFO] Merging blocks ends")

    f = open("document info ai.txt", "w")
    f.write(' '.join(ending_words)+"\n")
    f.write("Total document number:"+str(doc_number)+"\n")
    f.write("Average document length:" + str(lave))
    f.close()
