import yaml
import time
import datetime
import urllib.request

# from urllib.parse import urlparse

# parse yml to format:
# [{'delay': 60, 'url': 'http://wp.pl'},
#  {'delay': 180, 'url': 'http://o2.pl'},
#  {'delay': 75, 'url': 'http://pudelek.pl'}]
def parse_yml(yml_file, key):
    with open(yml_file) as urls_file:
        urls_dict = yaml.safe_load(urls_file)
    urls_list = urls_dict[key]
    return urls_list


def create_db_object(url):
    db_object = {}
    response_dict = {}

    # count response time
    start = time.clock()
    response = urllib.request.urlopen(url)
    response_time = time.clock() - start

    response_code = response.getcode()

    # creates dictionary
    response_dict['response_time'] = response_time
    response_dict['response_code'] = response_code
    response_dict['requesting_time'] = str(datetime.datetime.now())
    db_object['url'] = url
    db_object['requests'] = [response_dict]
    return db_object
