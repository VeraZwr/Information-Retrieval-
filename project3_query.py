import collections
import glob
import math
import sys
from project_1 import generate_tokens_pipeline

K = 1.2
b = 0.75
def query_parser(query: str):
    return sorted(generate_tokens_pipeline(query))


def find_file_index(spliting_words, term):
    if len(spliting_words) == 1 and spliting_words[0] == "":
        return 0
    for i in range(len(spliting_words)):
        if i + 1 < len(spliting_words) and spliting_words[i] < term <= spliting_words[i + 1]:
            return i + 1
        elif i == 0 and term <= spliting_words[i]:
            return 0
        elif i + 1 == len(spliting_words) and term >= spliting_words[i]:
            return i + 1

    return len(spliting_words)


def retrieve(index_files: str, words: list, splits: list, N, l_avg, verbose=False, rank_alg="bm25"):
    res = []
    if len(words) == 0:
        return res
    a = words[0]
    file_index = find_file_index(splits, a)
    f = open(index_files[file_index], "r")
    line = f.readline().strip("\n")
    while line:
        if line.split("`")[0] == a:
            res.append(line.rstrip("\n").split("`")[1].split(":-)"))
            if verbose:
                print("index: 0", line)
            break
        elif line.split("`")[0] > a:
            break
        else:
            line = f.readline().strip("\n")

    for i in range(1, len(words)):
        b_posting = []
        b = words[i]
        line = f.readline().strip("\n")
        while line:
            if line.split("`")[0] == b:
                b_posting = line.rstrip("\n").split("`")[1].split(":-)")
                if verbose:
                    print("index:", i, line)
                break
            elif line.split("`")[0] > b:
                break
            else:
                line = f.readline().strip("\n")
                if line == '':
                    f.close()
                    next_file_index = find_file_index(splits, b)
                    #print("dfsfsdfsdfds", next_file_index, "  ", b)
                    if file_index == next_file_index:
                        print("[INFO] No postings for ", b)
                        b_posting = []
                        break
                    f = open(index_files[next_file_index], "r")
                    line = f.readline().strip("\n")
                    file_index = next_file_index
        res.append(b_posting)
    f.close()
    res = rank(res, N, l_avg, rank_alg)
    if verbose:
       print("[INFO] Retrieved docs for each word in query: ", res)
    return res


def help():
    print("[Usage] python3 project_1_query.py -[rank_alg] [query:string] -[v:verbose]")


def cal_score_BM25(ld, tf, N, df, l_avg):
    # print(ld, tf, N, df, l_avg)
    return (math.log(N/df))*(K+1)*tf/(K*((1-b)+b*(ld/l_avg))+tf)

def cal_score_tf_idf(tf, N, df):
    # print(ld, tf, N, df, l_avg)
    return math.log(N / df)*(1 + math.log(tf))

def rank(postings, N ,l_avg, rank_alg): # use the different algorithm to calculate the score
    res = {}
    for posting in postings: #['url~337~1', 'url~389~2'...]
        for info in posting:
            # length of document
            url, ld, tf = [x for x in info.strip("\n").split("~")]  # [url, 337, 1]
            ld = int(ld)
            tf = int(tf)
            df = len(posting)
            if rank_alg == "-bm25":
                score = cal_score_BM25(ld, tf, N, df, l_avg)
            elif rank_alg == "-tfidf":
                score = cal_score_tf_idf(tf, N, df)
            if res.get(url, None) is not None:
                res[url] = res[url] + score
            else:
                res[url] = score
    sorted_res = sorted(res.items(), key=lambda kv: kv[1], reverse=True)  # kv[1]  = score
    #print(sorted_res[:10])
    # return [item[0] for item in sorted_res]
    return [item for item in sorted_res]



if __name__ == "__main__":
    f = open("spliting_word.txt", "r")
    spliting_words = f.readline().strip("\n").split(":-)")
    f = open("document info.txt", "r")
    document_info = f.readline().strip("\n").split(":-)")
    N = int(f.readline().strip("\n").split(":")[1])
    l_avg = int(f.readline().strip("\n").split(":")[1])
    f.close()
    print("[INFO] Document_info : ", document_info )
    print("[INFO] System arguments: ", sys.argv)

    files = sorted(glob.glob("./CaWebIndex/*.txt"))
    print(files)

    try:
        rank_alg = sys.argv[1]
        query = query_parser(sys.argv[2])
        if len(sys.argv) == 4 and sys.argv[3] == "-v":
            verbose = True
        else:
            verbose = False
        print("[INFO] Query: ", query)
        print("[INFO] Rank_Algorithm: ", rank_alg)
    except IndexError:
        print("Error! Please check command input")
        help()
        sys.exit(1)


    if rank_alg == "-bm25":
        res = retrieve(files, query, spliting_words, N, l_avg, verbose, rank_alg)
        print("[INFO]----Results------", res[:100])
    elif rank_alg =="-tfidf":
        res = retrieve(files, query, spliting_words, N, l_avg, verbose, rank_alg)
        print("[INFO]----Results------", res[:100])
       # print("[INFO]----Total Count------", len(res))
    else:
        print("[ERROR] missing boolean operator '-a' '-bm25' '-tfidf' ")
        help()
