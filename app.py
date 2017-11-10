import atexit
import pymongo
import json
from bson import json_util
from functools import partial
from flask import Flask, request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from utils import parse_yml, create_db_object

app = Flask(__name__)

scheduler = BackgroundScheduler()
scheduler.start()

client = pymongo.MongoClient()
db = client.database
collection = db.collection
elements = db.elements

urls = parse_yml('config.yml', 'urls')

# database removing in case of debugging:
# client.drop_database(db)


def insert_url_in_db(url):
    entry = create_db_object(url)
    # insert new element to database or update url's requests' list
    if not db.elements.find_one({"url": url}):
        entry_id = elements.insert_one(entry).inserted_id
    else:
        db.elements.update(
            {"url": url}, {'$addToSet': {'requests': entry['requests']}}
            )

for url_set in urls:
    scheduler.add_job(
        func=partial(insert_url_in_db, url=url_set['url']),
        trigger=IntervalTrigger(seconds=url_set['delay']),
        replace_existing=False)
# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())


@app.route('/')
def hello_world():
    return "Hello, homework!"


# retrieve complete collection
@app.route('/results', methods=['GET'], endpoint="get")
def get():
    cursor = db.elements.find()
    return json_util.dumps(cursor)


# retrieve document by url
@app.route('/results-by-url', methods=['POST'], endpoint="post")
def post():
    if not json.loads(request.data.decode("utf8")):
        raise ValueError("empty request body")

    data_dict = json.loads(request.data.decode("utf8"))
    url = data_dict["url"]
    cursor = db.elements.find({"url": url})
    return json_util.dumps(cursor)


if __name__ == "__main__":
    app.run()
