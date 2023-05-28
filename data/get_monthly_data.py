import requests
import json
import datetime

def get_answer(prompt):
    data = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {"role": "user", "content": prompt},
        ]
    }
    url = r'http://103.238.162.37:10071/chat_completion/no_cache'
    res = requests.post(url, json=data)
    return res.json()["choices"][0]["message"]["content"]

def collect_hc3_data(start, end):
    with open("/data/tsq/OpenChatLog/question_source/HC3_zh.jsonl", 'r', encoding='utf8') as f:
        hc3 = f.readlines()
    with open("/data/tsq/OpenChatLog/monthly_data/hc3_zh_may.jsonl", "a+", encoding="utf8") as outf:
        cnt = 0
        for line in hc3:
            data = json.loads(line)
            cnt += 1
            if cnt >= start:
                print(cnt)
                res = {
                    "id": cnt,
                    "source_type": "api",
                    "source_dataset": "HC3",
                    "source_task": data["source"],
                    "q": data["question"],
                    "a": get_answer(data["question"]),
                    "language": "zh",
                    "chat_date": str(datetime.date.today())
                }
                outf.write(str(json.dumps(res, ensure_ascii=False)) + "\n")
            if cnt == end:
                break

def collect_mwp_data():
    with open("/data/tsq/OpenChatLog/question_source/mwp.jsonl", 'r', encoding='utf8') as f:
        mwp = f.readlines()
    with open("/data/tsq/OpenChatLog/monthly_data/mwp_may.jsonl", "a+", encoding="utf8") as outf:
        for line in mwp:
            data = json.loads(line)
            print(data["id"])
            res = {
                "id": data["id"],
                "source_type": "api",
                "source_dataset": data["source_dataset"],
                "source_task": data["source_task"],
                "q": data["q"],
                "a": get_answer(data["q"]),
                "language": "en",
                "chat_date": str(datetime.date.today())
            }
            outf.write(str(json.dumps(res, ensure_ascii=False)) + "\n")

def collect_chatgpt_vs_bert():
    with open("/data/tsq/OpenChatLog/question_source/ChatGPT_vs_BERT.jsonl", 'r', encoding='utf8') as f:
        d = f.readlines()
    with open("/data/tsq/OpenChatLog/monthly_data/chatgpt_vs_bert_may.jsonl", "a+", encoding="utf8") as outf:
        for line in d:
            data = json.loads(line)
            print(data["id"])
            res = {
                "id": data["id"],
                "source_type": "api",
                "source_dataset": data["source_dataset"],
                "source_task": data["source_task"],
                "q": data["q"],
                "a": get_answer(data["q"]),
                "language": "en",
                "chat_date": str(datetime.date.today())
            }
            outf.write(str(json.dumps(res, ensure_ascii=False)) + "\n")

if __name__ == "__main__":
    collect_mwp_data()
    print("mwp Done!")
    collect_chatgpt_vs_bert()
    print("chatgpt_vs_bert Done!")