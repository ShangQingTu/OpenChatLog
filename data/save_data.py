import argparse
import os
from tqdm import tqdm
import time
import pandas as pd
import json

# 按论文发布时间算
dataset2date = {
    "HC3": "2023-01-18",
    "UltraChat": "2023-04-20",
    "GPT-4-LLM": "2023-04-06",
    "ArguGPT": "2023-04-16",
    "medAlpaca": "2023-04-07",
}


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
    parser.add_argument('--task', type=str, default='save',
                        choices=['save', 'update'])
    args = parser.parse_args()
    args.save_jsonl_dir = os.path.join(args.data_dir, args.source_type, args.time, args.language,
                                         args.source_dataset)
    if not os.path.exists(args.save_jsonl_dir):
        os.makedirs(args.save_jsonl_dir)
    args.save_jsonl_path = os.path.join(args.save_jsonl_dir, args.file_name)
    return args


class DataSaver:
    def __init__(self, args):
        self.args = args
        # input
        self.input_dir = os.path.join(args.data_dir, 'raw',
                                         args.source_dataset)
        # save
        self.save_jsonl_path = args.save_jsonl_path


    def save_jsons(self):
        # list files
        files = os.listdir(self.input_dir)
        data_num = 0
        for file in files:
            file_name = file.split(".")[0]
            json_objs = []
            if file.endswith("json"):
                # read
                with open(os.path.join(self.input_dir, file), 'r') as fin:
                    json_objs = json.load(fin)
            elif file.endswith("jsonl"):
                print(f"Reading from {self.input_dir}")
                print(f"Reading {file}")
                with open(os.path.join(self.input_dir, file), 'r') as fin:
                    lines = fin.readlines()
                    for line in lines:
                        json_objs.append(json.loads(line.strip()))
            else:
                continue
            source_task = file_name
            fout = open(self.save_jsonl_path, 'a')
            for iter, json_obj in enumerate(json_objs):
                if self.args.source_dataset == "UltraChat":
                    uid = json_obj["id"]
                    u_data = json_obj["data"]
                    iter_num = len(u_data) // 2
                    for i in range(iter_num):
                        question = u_data[i * 2]
                        chatgpt_answer = u_data[i * 2 + 1]
                        json_obj = {
                            "id": "ulrta"+uid+str(i),
                            "source_type": self.args.source_type,
                            "source_dataset": self.args.source_dataset,
                            "source_task": source_task,
                            "q": question,
                            "a": chatgpt_answer,
                            "language": self.args.language,
                            "chat_date": dataset2date[self.args.source_dataset],
                            "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        }
                        # log
                        data_num += 1
                        fout.write(json.dumps(json_obj, ensure_ascii=False) + "\n")
                elif self.args.source_dataset == "medAlpaca":
                    question = json_obj["instruction"] + ' ' + json_obj["input"]
                    chatgpt_answer = json_obj["output"]
                    json_obj = {
                        "id": "medA" + str(iter),
                        "source_type": self.args.source_type,
                        "source_dataset": self.args.source_dataset,
                        "source_task": source_task,
                        "q": question,
                        "a": chatgpt_answer,
                        "language": self.args.language,
                        "chat_date": dataset2date[self.args.source_dataset],
                        "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    }
                    # log
                    data_num += 1
                    fout.write(json.dumps(json_obj, ensure_ascii=False) + "\n")

        return data_num
    
    def save_argugpt(self):
        # list files
        self.input_dir = os.path.join(self.input_dir, 'data', 'argugpt')
        files = ['machine-train.csv', 'machine-dev.csv', 'machine-test.csv']
        data_num = 0
        for file in files:
            file_name = file.split(".")[0]
            df = pd.read_csv(os.path.join(self.input_dir, file))
            source_task = "argument-writing"
            fout = open(self.save_jsonl_path, 'a')
            for iter, row in df.iterrows():
                question = row["prompt"]
                chatgpt_answer = row["text"]
                json_obj = {
                    "id": "argu-" + file_name + "-" +row["model"] +str(iter),
                    "source_type": self.args.source_type,
                    "source_dataset": self.args.source_dataset,
                    "source_task": source_task,
                    "q": question,
                    "a": chatgpt_answer,
                    "language": self.args.language,
                    "chat_date": dataset2date[self.args.source_dataset],
                    "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                }
                # log
                data_num += 1
                fout.write(json.dumps(json_obj, ensure_ascii=False) + "\n")
        return data_num

    def save_gpt4llm(self):
        # list files
        self.input_dir = os.path.join(self.input_dir, 'data')
        files = ['alpaca_gpt4_data.json', 'unnatural_instruction_gpt4_data.json', 'alpaca_gpt4_data_zh.json']
        data_num = 0
        for file in files:
            file_name = file.split(".")[0]
            if file_name.endswith("zh"):
                self.args.language = "zh"
            json_objs = []
            # read
            with open(os.path.join(self.input_dir, file), 'r') as fin:
                json_objs = json.load(fin)
                
            source_task = file_name
            fout = open(self.save_jsonl_path, 'a')
            for iter, json_obj in enumerate(json_objs):
                question = json_obj["instruction"] + ' ' + json_obj["input"]
                chatgpt_answer = json_obj["output"]
                json_obj = {
                    "id": "medA" + str(iter),
                    "source_type": self.args.source_type,
                    "source_dataset": self.args.source_dataset,
                    "source_task": source_task,
                    "q": question,
                    "a": chatgpt_answer,
                    "language": self.args.language,
                    "chat_date": dataset2date[self.args.source_dataset],
                    "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                }
                # log
                data_num += 1
                fout.write(json.dumps(json_obj, ensure_ascii=False) + "\n")
        return data_num

    def save(self):
        if self.args.source_dataset == 'ArguGPT':
            data_num = self.save_argugpt()
        elif self.args.source_dataset == 'GPT-4-LLM':
            data_num = self.save_gpt4llm()
        else:
            data_num = self.save_jsons()
        print(f"Finish saving {data_num} data into {self.save_jsonl_path}")

    def update(self):
        pass


class DBLoader:
    def __init__(self, args):
        self.args = args
        # data
        self.input_josnl_dir = os.path.join(args.data_dir, args.source_type, args.time, args.language,
                                            args.source_dataset)
        self.input_jsonl_path = os.path.join(self.input_josnl_dir, args.file_name)
        # where to load features
        self.feature_dir = os.path.join(self.args.data_dir, "api", "after0301",
                                        self.args.language,
                                        "HC3_features")

    def load_questions(self):
        questions = []
        with open("/data/tsq/CK/data/open/before0301/en/HC3/HC3_en.jsonl", 'r') as fin:
            lines = fin.readlines()[self.args.start_id:self.args.end_id]
            for line in lines:
                json_obj = json.loads(line.strip())
                question = json_obj["question"]
                questions.append(question)
        return questions

    def load_by_questions(self, time_qualifier):
        raw_answers = []
        res_lst = []
        with open(self.input_jsonl_path, 'r') as fin:
            lines = fin.readlines()[self.args.start_id:self.args.end_id]
            for line in tqdm(lines, total=len(lines)):
                json_obj = json.loads(line.strip())
                # get source_task, q and a
                if self.args.source_dataset == "HC3" and self.args.source_type == "open":
                    question = json_obj["question"]
                else:
                    question = json_obj["q"]
                # search in DB by question
                query = {"q": question}
                # print("query")
                # print(query)
                # get from mdb
                _res = self.mdb.get_data(query)
                res_lst = list(_res)
                for res in res_lst:
                    # print("res")
                    # print(res)
                    # print("#" * 8)
                    if res["chat_date"] == time_qualifier:
                        raw_answers.append(res["a"])
                        res_lst.append(dict(res))
                        break

        return raw_answers, res_lst

    def load_by_json(self, time_qualifier, pp_suffix=""):
        mm_dd = time_qualifier[-5:]
        if pp_suffix:
            suffix = pp_suffix
        else:
            suffix = "base"
        if self.args.source_dataset == "HC3" and self.args.source_type == "open":
            input_jsonl_path = self.input_jsonl_path
        else:
            input_jsonl_path = os.path.join(self.input_josnl_dir, f"data{mm_dd}_{suffix}.jsonl")
        raw_answers = []
        res_lst = []
        with open(input_jsonl_path, 'r') as fin:
            lines = fin.readlines()[self.args.start_id:self.args.end_id]
            for line in tqdm(lines, total=len(lines)):
                json_obj = json.loads(line.strip())
                # get source_task, q and a
                if self.args.extract_source == "chatgpt_answers":
                    if self.args.source_dataset == "HC3" and self.args.source_type == "open":
                        answer = json_obj["chatgpt_answers"][0]
                    else:
                        answer = json_obj["a"]
                    # answer
                    raw_answers.append(answer)
                else:
                    raw_answers.extend(json_obj["human_answers"])
                res_lst.append(json_obj)
        if self.args.extract_source == "chatgpt_answers":
            save_json_path = os.path.join(self.feature_dir, f"feature{mm_dd}_{suffix}.json")
        else:
            print(f"raw_answers: {len(raw_answers)}")
            save_json_path = os.path.join(self.feature_dir, f"feature{mm_dd}_human.json")
        return raw_answers, res_lst, save_json_path

    def load_feature_by_json(self, times, pp_suffixes):
        # set time_qualifier
        print(f"times: {times}")
        if times == ["all"] or times == "all":
            print(self.feature_dir)
            files = os.listdir(self.feature_dir)
            names = []
            for file in files:
                if file.startswith("feature"):
                    names.append(file.split("_")[0])
            # filter complicate days
            final_names = set(names)
            time_qualifiers = list(final_names)
            time_qualifiers.sort()
        else:
            time_qualifiers = times

        # features dict, k1: pp_suffix, k2: time_qualifier, v: feature(also a dict load from json)
        features_dict = {}
        for pp_suffix in pp_suffixes:
            pp_features_dict = {}
            for time_qualifier in time_qualifiers:
                mm_dd = time_qualifier[-5:]
                file_name = f"feature{mm_dd}_{pp_suffix}.json"
                file_path = os.path.join(self.feature_dir, file_name)
                with open(file_path, 'r') as fin:
                    feature_obj = json.load(fin)
                    pp_features_dict[mm_dd] = feature_obj

            features_dict[pp_suffix] = pp_features_dict
        return features_dict


if __name__ == '__main__':
    args = prepare_args()
    data_saver = DataSaver(args)
    if args.task == 'save':
        data_saver.save()
    elif args.task == 'update':
        data_saver.update()
