import os
import re
import time
import json
import pandas as pd
from tqdm import tqdm
from urllib import parse
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, RequestsHttpConnection

############################## Global Variables ##############################

# ElasticSearch Info
ES_HOST = ""
ES_ID = ""
ES_PW = ""

# Log Info
LOG_INDEX = ""
LOG_FIELD = ""
LOG_TEXT = ""

# 로그 시작 날짜 시간
TIME_PERIOD_START = "2022-06-06 00:00:00.000"

# 로그 끝 날짜 시간
TIME_PERIOD_END = "2022-06-07 23:59:59.999"

# 검색 개수 제한
SIZE = 100

# 검색 대상 API 목록
API_LIST = ['']

# 결과 생성 폴더 이름
DIRNAME = "Test Case"

# Environment
PROFILE = "dev"

##############################################################################


def date_range(start, end):
    start = datetime.strptime(start, "%Y-%m-%d")
    end = datetime.strptime(end, "%Y-%m-%d")
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end-start).days+1)]


def get_doc(api_name):
    # 접속할 elastic 정보
    es = Elasticsearch(
        ES_HOST,
        http_auth=(ES_ID, ES_PW),
        use_ssl=True,
        verify_cert=True,

        headers={
            "x-user-auth": "",
            "Content-Type": "application/x-ndjson"
        },
        connection_class=RequestsHttpConnection
    )

    body = {
            "size": SIZE,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                ""
                                "@timestamp": {
                                    "gte": re.sub('\s', 'T', TIME_PERIOD_START) + "+09:00",
                                    "lte": re.sub('\s', 'T', TIME_PERIOD_END) + "+09:00"
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "@timestamp": {
                        "order": "asc"
                    }
                }
            ]
        }

    for name in api_name.split('/'):
        if name:
            body["query"]["bool"]["must"].append({"match": {LOG_FIELD: name}})

    index_list = []
    for date in date_range(TIME_PERIOD_START.split(' ')[0], TIME_PERIOD_END.split(' ')[0]):
        index_list += es.indices.get(LOG_INDEX + date)

    resp = es.search(
        index=sorted(list(set(index_list))),
        body=body,
        scroll='1m'
    )

    # 처음 출력된 결과 저장
    result = []
    for doc in resp['hits']['hits']:
        result.append(doc['_source'])

    return result


def make_file(api_name):
    file_data = {
        "info": {
            "name": '{} test'.format(api_name),
            "description": '{} test'.format(api_name),
            "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
        },
        "item": []
    }

    api_data = {
        "name": api_name,
        "item": []
    }

    response_list = []
    for index, log in enumerate(get_doc(api_name)):
        response_list.append(["{api_name}_{index}".format(api_name=api_name, index=index + 1), log["request_time"]])

        log_data = {
            "name": "{api_name}_{index}".format(api_name=api_name, index=index+1),
            "event": [
                {
                    "listen": "test",
                    "script": {
                        "exec": [""],
                        "type": "text/javascript"
                    }
                }
            ],
            "request": {
                "method": '{method}'.format(method=log["method"]),
                "header": [],
                "url": {
                    "raw": '{url}'.format(url="https://" + "{{" + "{host}".format(host=log["service_type"].lower()) + "}}" + parse.unquote(parse.unquote(log["log"].split(' ')[6]))),
                    "protocol": "https",
                    "host": ["{{" + "{service_type}".format(service_type=log["service_type"].lower()) + "}}"],
                    "path": [path for path in log["log"].split(' ')[6].split('?')[0].split('/')[1:]],
                    "query": [{"key": p.split("=")[0], "value": '='.join(p.split("=")[1:])} for p in parse.unquote(parse.unquote(log["log"].split(' ')[6].split('?')[1])).split("&")] if len(log["log"].split(' ')[6].split('?')) > 1 else []
                }
            },
            "response": []
        }
        api_data["item"].append(log_data)

    # 갯수 제한
    api_data['item'] = api_data['item'][:SIZE]
    response_list = response_list[:SIZE]

    file_data["item"].append(api_data)

    return file_data, response_list


def main():
    # 결과 저장 폴더 없으면 생성
    root_dir = DIRNAME
    if root_dir not in os.listdir():
        os.mkdir(root_dir)

    # postman, csv 폴더 없으면 생성
    postman_dir = 'postman'
    csv_dir = 'csv'
    for dir_name in [postman_dir, csv_dir]:
        if dir_name not in os.listdir(root_dir):
            os.mkdir('{}/{}'.format(root_dir, dir_name))

    # API 마다 실행
    for api_name in tqdm(API_LIST):
        # API 별로 로그 검색
        file_data, response_list = make_file(api_name)

        # Postman Collection 파일 이름
        postman_file = '{}.json'.format(api_name.replace("/", "_"))

        # postman file이 이미 있으면 삭제
        if postman_file in os.listdir('{}/{}'.format(root_dir, postman_dir)):
            os.remove('{}/{}/{}'.format(root_dir, postman_dir, postman_file))

        # postman collection json 파일 생성
        with open("{}/{}/{}".format(root_dir, postman_dir, postman_file), "w", encoding="utf-8") as postman_collection:
            json.dump(file_data, postman_collection)

        # CSV 파일 이름
        csv_file = '{}.csv'.format(api_name.replace("/", "_"))

        # csv file이 이미 있으면 삭제
        if csv_file in os.listdir('{}/{}'.format(root_dir, csv_dir)):
            os.remove('{}/{}/{}'.format(root_dir, csv_dir, csv_file))

        # CSV 파일 내 값이 없으면 비어있는 값으로 입력
        if response_list:
            name, value = zip(*response_list)
        else:
            name, value = [''], ['']

        # CSV 파일 생성
        csv_dict = pd.DataFrame({"Test Case": name, "Response Time": value})
        csv_dict = csv_dict.reindex(range(len(csv_dict)))
        csv_dict.to_csv("{}/{}/{}".format(root_dir, csv_dir, csv_file))

        # 10초 딜레이 => elasticsearch에 너무 자주 검색하면 연결 끊김
        time.sleep(10)


if __name__ == '__main__':
    main()
