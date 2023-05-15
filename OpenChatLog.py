import streamlit as st
from data.database import MongoDB, TEST_VERSION
from copy import deepcopy
from difflib import SequenceMatcher
import plotly.graph_objs as go
import requests
import pandas as pd
import time
from data.backend import search
from streamlit_authenticator.authenticate import Authenticate


st.set_page_config(
    page_title="OpenChatLog",
    page_icon=":fox_face:"
)
mongo_url = "CONFIG.mongo_chatbot_uri"
mdb = MongoDB(collection_name=f"{TEST_VERSION}_detection_result", url=mongo_url)
mdb_user = MongoDB(collection_name=f"{TEST_VERSION}_user_label", url=mongo_url)

type2placeholder = {
    "Question for Answer":"What is ChatGPT?", 
    "Answer for Style":"ChatGPT is an AI language model developed by OpenAI.",
    "Role for Prompt":"Rich"
}

lang2word = {
    "submit": {
        "en": "predict",
        "zh": "预测"
    },
    "q": {
        "en": "Question",
        "zh": "问题"
    },
    "a": {
        "en": f"Answer written by {TEST_VERSION} or Human (to detect)",
        "zh": f"由{TEST_VERSION}或人写的回答 (待检测)"
    },
    "classify_by_user": {
        "en": "The class of your submitted answer",
        "zh": "您所提交回答的类型"
    },
    "confirm": {
        "en": "Confirm (please click twice)",
        "zh": "确认 (请双击)"
    },
    "pic": {
        "en": "Upload screenshot of ChatGPT",
        "zh": "上传ChatGPT聊天截图"
    },
    "time": {
        "en": "When did you get the ChatGPT answer?",
        "zh": "您获取ChatGPT回答的时间?"
    }
}


def query(payload):
    if TEST_VERSION == "ChatGPT":
        # [ 关键词抽取比较, fine-tune好的classifier] 的结果
        response0 = requests.post("knowledge_extraction_api", json=payload)
        similar_history_chats = response0.json()

        # score by classifier
        response = requests.post("classification_api", json=payload)
        classifier_score = response.json()
        detection_result = [similar_history_chats, classifier_score]
        return detection_result
    else:
        response = requests.post("", json=payload)
        return response.json()


def get_text():
    q_input_text = st.text_area(f"{lang2word['q'][st.session_state['language']]}: ", key="q_input")
    a_input_text = st.text_area(f"{lang2word['a'][st.session_state['language']]}: ", key="a_input")
    input_text = {"q": q_input_text, "a": a_input_text}
    return input_text


def ask_for_labeling(_record):
    # User's label
    user_label = st.selectbox(
        f"{lang2word['classify_by_user'][st.session_state['language']]}: ",
        ('human', 'ChatGPT', 'mixed')
    )
    bytes_data = None
    date = None
    if user_label != 'human':
        # Picture
        uploaded_file = st.file_uploader(f"{lang2word['pic'][st.session_state['language']]}:", type="jpg")
        if uploaded_file:
            bytes_data = uploaded_file.read()
        # Date
        date = st.date_input(f"{lang2word['time'][st.session_state['language']]}")

    # Finish
    _id = mdb_user.get_size()
    record = {"id": _id, "cdr_id": _record["id"]}
    if st.button(f"{lang2word['confirm'][st.session_state['language']]}", key="final"):
        # add into db
        if user_label:
            record["user_label"] = user_label
        if user_label != 'human':
            if bytes_data:
                record["pic"] = bytes_data
            if date:
                record["chat_date"] = date.strftime('%Y-%m-%d')
        # log
        # print(record)
        mdb_user.add_one(record)
        st.session_state['generated'] = []


def html_diff(text1, text2, _name):
    # d = difflib.HtmlDiff()
    # html_content = d.make_file(text1, text2)
    # `print`(html_content)
    # st.markdown(html_content, unsafe_allow_html=True)
    seq = SequenceMatcher(None, text1, text2)
    st.markdown(f"*User {_name}* is: ")
    st.markdown(f"**{text1}**")
    st.markdown(f"*Matched {_name}* is:")
    st.markdown(f"**{text2}**")
    st.markdown(f"Result of {_name} comparison for each word:")
    for opcode in seq.get_opcodes():
        st.markdown("> %6s user[%d:%d] history[%d:%d]" % opcode)


def visualize_similarity(user_q, user_a, similar_history_res, sim_by):
    similarity = int(similar_history_res['prob'] * 1000) / 10
    history_id = similar_history_res['index']
    st.markdown(f"This text is matched by {sim_by} with a similarity of {similarity} %.")
    # get q&a text from database
    collection = f"{TEST_VERSION}_history_record_{st.session_state['language']}"
    mdb_history = MongoDB(collection_name=collection,
                          url=mongo_url)
    _res = mdb_history.get_data({"id": int(history_id)})
    res_lst = list(_res)
    # print(res_lst)
    for res in res_lst:
        sim_q = res['q']
        sim_a = res['a']
        # show diff
        html_diff(user_q, sim_q, "question")
        html_diff(user_a, sim_a, "answer")


def visualize_classify(probs, label):
    classify_prob = probs[0]
    question_matching_prob = probs[1]
    answer_matching_prob = probs[2]
    if label == "human":
        class_h_prob = classify_prob
        class_c_prob = 100 - classify_prob
    else:
        class_h_prob = 100 - classify_prob
        class_c_prob = classify_prob
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            y=['answer similarity', 'question similarity', 'classifier probability'],
            # y=['probability'],
            x=[answer_matching_prob, question_matching_prob, class_c_prob],
            # x=[prob],
            name=TEST_VERSION,
            orientation='h',
            marker=dict(color='rgba(246, 78, 139, 0.6)',
                        line=dict(color='rgba(246, 78, 139, 1.0)'))))
    fig.add_trace(
        go.Bar(
            y=['answer similarity', 'question similarity', 'classifier probability'],
            # y=['probability'],
            x=[100 - answer_matching_prob, 100 - question_matching_prob, class_h_prob],
            # x=[100 - prob],
            name="human",
            orientation='h',
            marker=dict(color='rgba(58, 71, 80, 0.6)',
                        line=dict(color='rgba(58, 71, 80, 1.0)'))))
    fig.update_layout(barmode='stack')
    # fig.update_traces(bargap=3)
    st.markdown(f"This text is written by **{label}**.")
    st.plotly_chart(fig, use_container_width=True)


def visualize_pred_res(user_q, user_a, output):
    """
    :param user_q: str
    :param user_a: str
    :param output: [similar_history_chats, classifier_score]
           similar_history_chats = {"q": q_res, "a": a_res}, a_res = {'prob': prob, 'index': history_index}
           classifier_score = {'prob': prob, 'label': label}
    :return: visualization
    """
    # visualize classify
    prob = output[1]["prob"]
    label = output[1]["label"]
    similar_history_q = output[0]["q"]
    similar_history_a = output[0]["a"]
    probs = [prob, similar_history_q['prob'] * 100, similar_history_a['prob'] * 100]
    visualize_classify(probs, label)
    # visualize similarity
    tab1, tab2 = st.tabs(["Most Similar Question(default)", "Most Similar Answer"])
    with tab1:
        visualize_similarity(user_q, user_a, similar_history_q, "q")
    with tab2:
        visualize_similarity(user_q, user_a, similar_history_a, "a")


def main_page():
    # creating a login widget

    st.write(f'Welcome to OpenChatLog! There are 3 query types for users to cunstomize:')
    st.markdown("> 1. Given a question, provide users with answer candidates from ChatGPT histroy database with colorful styles.\n"
                +
                "> 2. Given a piece of text (answer from ChatGPT), provide users with the most matching ChatGPT style and text in database.\n > "+
                "3. Given a kind of role (e.g. storyteller), provide users with recommended combinations of decoding parameters and prompting"+
                " templates that may instruct ChatGPT to fit this role.")
    st.title("ChatGPT Style Search Engine")

    # Connect to the Google Sheet
    data_path = "/data/tsq/CK/pic/avg_HC3_all_pearson_corr.csv"
    df = pd.read_csv(data_path, dtype=str).fillna("")
    # print(df.columns)
    # Use a text_input to get the keywords to filter the dataframe
    c1, c2 = st.columns([2,8])
    # Add options
    talk_options = ["Question for Answer", "Answer for Style", "Role for Prompt"]
    type_sel = c1.selectbox("Query Type", talk_options)
    st.session_state['placeholder'] = type2placeholder[type_sel]
    text_search = c2.text_input("Search Query", value="", placeholder=st.session_state.placeholder)
    # Show the cards
    N_cards_per_row = 1
    if text_search:
        # Show the results, if you have a text_search
        m1 = df["Unnamed: 0"].str.contains(text_search)
        df_search = df[m1]
        for n_row, row in df_search.reset_index().iterrows():
            i = n_row%N_cards_per_row
            if i==0:
                st.write("---")
                cols = st.columns(N_cards_per_row, gap="large")
            # draw the card
            with cols[n_row%N_cards_per_row]:
                st.caption(f"{row['Unnamed: 0'].strip()} - {row[row['Unnamed: 0']].strip()} ")
                st.markdown(f"**{row['WRich05_S'].strip()}**")
                st.markdown(f"*{row['ppl'].strip()}*")
                st.markdown(f"**{row['rouge-l-f']}**")


if __name__ == '__main__':
    main_page()