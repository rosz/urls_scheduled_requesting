import atexit
import pymongo
from bson import json_util
from flask import Flask, request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from functools import partial
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
    return 'Hello, World!'


@app.route('/results', methods=['GET'])
def get():
    if request.method == 'GET':
        cursor = db.elements.find()
    return json_util.dumps(cursor)


if __name__ == "__main__":
    app.run()
