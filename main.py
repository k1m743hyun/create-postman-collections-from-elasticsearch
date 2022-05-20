import os
import re
import csv
import pip
import http
import time
import json
import requests
import pandas as pd
from tqdm import tqdm
from urllib import parse
from wsgiref import headers
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, exceptions, RequestsHttpConnection

############################## 검색 조건 ##############################
TIME_PERIOD_START = "2022-05-01 00:00:00.000"
TIME_PERIOD_END = "2022-05-02 23:59:59.999"

# 검색 개수 제한
SIZE = 1000

# 검색 대상 API
API_LIST = ['']

DIRNAME = 'Test Case'

# 검색 환경
PROFILE = "dev"

# 검색 대상 INDEX
NGINX_INDEX = ""

# 검색 대상 FIELD 및 TEXT
NGINX_FIELD = "api"
NGINX_TEXT = ""

###### ElasticSearch Info ######
ES_HOST = ""
ES_ID = ""
ES_PW = ""
#####################################################################


def DOC_FIELD(doc):
    return doc["_source"]


def get_doc(TIME_PERIOD_START, TIME_PERIOD_END, INDEX, FIELD, TEXT):
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

    for T in TEXT.split('/'):
        if T:
            body["query"]["bool"]["must"].append({"match": {FIELD: T}})

    resp = es.search(
        index=INDEX,
        body=body,
        scroll='1m'
    )

    old_scroll_id = resp['_scroll_id']

    result = []

    # 처음 출력된 결과 저장
    for doc in resp['hits']['hits']:
        result.append(DOC_FIELD(doc))

    # SCROLL API를 통해 나온 결과 저장
    if len(result) < COUNT_LIMIT:
        while len(resp['hits']['hits']):
            resp = es.scroll(
                scroll_id=old_scroll_id,
                scroll='1m'  # length of time to keep search context
            )
            for doc in resp['hits']['hits']:
                if FIELD != "":
                    result.append(DOC_FIELD(doc))
                else:
                    result.append(DOC_FIELD(doc))

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

    response_list = []

    api_data = {
        "name": api_name,
        "item": []
    }

    for index, log in enumerate(get_doc(TIME_PERIOD_START, TIME_PERIOD_END, NGINX_INDEX + TIME_PERIOD_START.split(' ')[0], NGINX_FIELD, api_name)):
        response_list.append(["{api_name}_{index}".format(api_name=api_name, index=index + 1), log["request_time"]])

        log_data = {
            "name": "{api_name}_{index}".format(api_name=api_name, index=index+1),
            "event": [
                {
                    "listen": "test",
                    "script": {
                        "exec": [],
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

    file_data["item"].append(api_data)

    return file_data, response_list


def main():
    root_dir = DIRNAME
    if root_dir not in os.listdir():
        os.mkdir(root_dir)

    postman_dir = 'postman'
    csv_dir = 'csv'
    for dir_name in [postman_dir, csv_dir]:
        if dir_name not in os.listdir(root_dir):
            os.mkdir('{}/{}'.format(root_dir, dir_name))

    for api_name in tqdm(API_LIST):
        file_data, response_list = make_file(api_name)

        # Postman Collection
        postman_file = '{}.json'.format(api_name.replace("/", "_"))

        # postman file이 이미 있으면 삭제
        if postman_file in os.listdir('{}/{}'.format(root_dir, postman_dir)):
            os.remove('{}/{}/{}'.format(root_dir, postman_dir, postman_file))

        with open("{}/{}/{}".format(root_dir, postman_dir, postman_file), "w", encoding="utf-8") as postman_collection:
            json.dump(file_data, postman_collection)

        # CSV
        csv_file = '{}.csv'.format(api_name.replace("/", "_"))

        # csv file이 이미 있으면 삭제
        if csv_file in os.listdir('{}/{}'.format(root_dir, csv_dir)):
            os.remove('{}/{}/{}'.format(root_dir, csv_dir, csv_file))

        if response_list:
            name, value = zip(*response_list)
        else:
            name, value = [''], ['']

        csv_dict = pd.DataFrame({"Test Case": name, "Response Time": value})
        csv_dict.to_csv("{}/{}/{}".format(root_dir, csv_dir, csv_file))

        time.sleep(10)


if __name__ == '__main__':
    main()
