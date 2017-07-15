import urllib.request
import requests
import logging


def get_request_and_jsonize(url):
    while True:
        try:
            search = requests.get(url)
            return search.json()
        except Exception as excpt:
            logging.error('%s', excpt)


def post_request_and_jsonize(url, data):
    while True:
        try:
            search = requests.post(url, data=data)
            return search.json()
        except Exception as excpt:
            logging.error('%s', excpt)


def safe_retrieve(url, nome):
    count = 1
    while count < 3:
        try:
            urllib.request.urlretrieve(url, nome)
            return
        except Exception as excpt:
            logging.error('%s', excpt)
            count += 1


def get_request_and_stringize(url):
    while True:
        try:
            search = requests.get(url)
            return search.content.decode('utf8')
        except Exception as excpt:
            logging.error('%s', excpt)


def post_request_and_stringize(url, data):
    while True:
        try:
            search = requests.post(url, data=data)
            return search.content.decode('utf8')
        except Exception as excpt:
            logging.error('%s', excpt)


