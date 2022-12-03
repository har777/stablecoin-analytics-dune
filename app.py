import json
import os
from flask import Flask
from flask_cors import cross_origin
import redis
from queries import get_all_blacklisted_events, get_all_unblacklisted_events, get_all_destroy_blacklisted_funds_events, get_all_mints, get_all_burns, get_all_transfers_involving_particular_user

from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

r = redis.Redis(host=os.getenv("REDIS_HOST"), port=int(os.getenv("REDIS_PORT")), password=os.getenv("REDIS_PASSWORD"))


@app.route("/blacklisted_events")
@cross_origin()
def blacklisted_events():
    key = "blacklisted_events"
    data = r.get(key)
    if not data:
        data = get_all_blacklisted_events()
        r.setex(key, 3600, json.dumps(data))
    else:
        data = json.loads(data)
    return data


@app.route("/unblacklisted_events")
@cross_origin()
def unblacklisted_events():
    key = "unblacklisted_events"
    data = r.get(key)
    if not data:
        data = get_all_unblacklisted_events()
        r.setex(key, 3600, json.dumps(data))
    else:
        data = json.loads(data)
    return data


@app.route("/destroyed_blacklisted_funds_events")
@cross_origin()
def destroyed_blacklisted_funds_events():
    key = "destroyed_blacklisted_funds_events"
    data = r.get(key)
    if not data:
        data = get_all_destroy_blacklisted_funds_events()
        r.setex(key, 3600, json.dumps(data))
    else:
        data = json.loads(data)
    return data


@app.route("/mint_events")
@cross_origin()
def mint_events():
    key = "mint_events"
    data = r.get(key)
    if not data:
        data = get_all_mints()
        r.setex(key, 3600, json.dumps(data))
    else:
        data = json.loads(data)
    return data


@app.route("/burn_events")
@cross_origin()
def burn_events():
    key = "burn_events"
    data = r.get(key)
    if not data:
        data = get_all_burns()
        r.setex(key, 3600, json.dumps(data))
    else:
        data = json.loads(data)
    return data


@app.route("/transfers/<user>")
@cross_origin()
def transfers_involving_user(user):
    key = f"transfers_involving_user-{user}"
    data = r.get(key)
    if not data:
        data = get_all_transfers_involving_particular_user(user)
        r.setex(key, 3600, json.dumps(data))
    else:
        data = json.loads(data)
    return data
