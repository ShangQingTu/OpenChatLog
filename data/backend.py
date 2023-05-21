from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text, connections
from elasticsearch_dsl import MultiSearch, Search, Q
import argparse
import os
import json
from tqdm import tqdm
# Define a default Elasticsearch client
connections.create_connection(hosts=['localhost:9344'])

            

class ChatLog(Document):
    type = Text(analyzer='snowball', fields={'raw': Keyword()})
    dataset = Text(analyzer='snowball', fields={'raw': Keyword()})
    task = Text(analyzer='snowball', fields={'raw': Keyword()})
    q = Text(analyzer='snowball')
    a = Text(analyzer='snowball')
    language = Text(analyzer='snowball', fields={'raw': Keyword()})
    chat_date = Date()
    lines = Integer()
    
    class Index:
        name = 'chatlog'
        settings = {
          "number_of_shards": 2,
        }

    def save(self, ** kwargs):
        self.lines = len(self.a.split())
        return super(ChatLog, self).save(** kwargs)

    def is_published(self):
        return datetime.now() > self.chat_date


def init(args):
    # create the mappings in elasticsearch
    if args.task == "init":
        ChatLog.init()

    # data
    with open(args.save_jsonl_path, 'r') as fin:
        lines = fin.readlines()
        for i, line in tqdm(enumerate(lines), total=len(lines)):
            json_obj = json.loads(line.strip())
            # json_obj = {
            #         "id": "chatgpt-12",
            #         "source_type": "open",
            #         "source_dataset": "HC3",
            #         "source_task": "ELI5",
            #         "q": "What is the meaning of life?",
            #         "a": "42",
            #         "language": "en",
            #         "chat_date": "2023-06-01"
            # }    

            # create and save and article
            try:
                _id = json_obj["id"]
            except KeyError:
                _id = str(i)
            # print(json_obj)
            new_id = _id + "_" + json_obj["source_dataset"]
            if json_obj["chat_date"].startswith("2023"):
                date_time = json_obj["chat_date"]
            else:
                date_time = "2023-" + json_obj["chat_date"]
            article = ChatLog(meta={'id': new_id}, 
                              type=json_obj["source_type"],
                              dataset=json_obj["source_dataset"], 
                              task=json_obj["source_task"], 
                              q=json_obj["q"], 
                              a=str(json_obj["a"]), 
                              language=json_obj["language"], 
                              chat_date=date_time)
            article.save()

    # Display cluster health
    print(connections.get_connection().cluster.health())

def search(query, field):
    client = Elasticsearch(hosts=['localhost:9344'])
    if field == "q":
        q = Q('bool',
                must=[Q('match', q=query)],
                # filter=[~Q('match', dataset='medAlpaca')],
                # should=[Q(...), Q(...)],
                # minimum_should_match=1
            )
        # s = Search().using(client).query("match", q=query)
        s = Search().using(client).query(q)
        # s = s.filter('terms', tags=['search', 'python'])
    elif field == "a":
        s = Search().using(client).query("match", a=query)
    elif field == "type":
        s = Search().using(client).query("match", type=query)
    elif field == "dataset":
        s = Search().using(client).query("match", dataset=query)
    elif field == "task":
        s = Search().using(client).query("match", task=query)
    elif field == "language":
        s = Search().using(client).query("match", language=query)
    elif field == "chat_date":  
        s = Search().using(client).query("match", chat_date=query)  
    else:   
        s = Search().using(client).query("match", q=query)
    response = s.execute()
    # print('Total %d hits found.' % s.count())
    return s

    # for hit in s:
    #     print(hit.chat_date)
    #     print(hit.q)
    #     print(hit.a)


def prepare_args():
    parser = argparse.ArgumentParser(description='Save ChatGPT QA data into mongoDB')
    parser.add_argument('--data_dir', help='Where to load', default='/data/tsq/OpenChatLog')
    parser.add_argument('--source_type', help='open or api', default='open',
                        choices=['open', 'api'])
    parser.add_argument('--time', help='When is the chat', default='before0301')
    parser.add_argument('--language', help='en/zh', default='en',
                        choices=['en', 'zh'])
    parser.add_argument('--source_dataset', help='Which dataset', default='HC3')
    parser.add_argument('--file_name', help='Which dataset', default='data.jsonl')
    # task
    parser.add_argument('--task', type=str, default='update',
                        choices=['init', 'update'])
    args = parser.parse_args()
    args.save_jsonl_dir = os.path.join(args.data_dir, args.source_type, args.time, args.language,
                                         args.source_dataset)
    if not os.path.exists(args.save_jsonl_dir):
        quit("save_jsonl_dir not exists")
    args.save_jsonl_path = os.path.join(args.save_jsonl_dir, args.file_name)
    return args

if __name__ == '__main__':
    args = prepare_args()
    init(args)
