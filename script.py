from typing import Optional
from pydantic import BaseModel
from fastapi import FastAPI
import datetime
import random
from multiprocessing import Lock
from uuid import uuid4
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

import os

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from SqlAlchemyTables import *


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db_string = os.environ["DB_STRING"]
engine = create_engine(db_string, echo=False, pool_size=1)
Session = sessionmaker(bind=engine)

# Create all tables
Base.metadata.create_all(engine)

session = Session()


def update_metrics():
    """
    For HOURLY_LIKES, HOURLY_VISITS, and HOURLY_USERS
    Create metrics from the hour after the last metric up to now
    """

    session = Session()

    last_metric = session.query(Metric).order_by(Metric.time.desc()).first()
    print(last_metric.time)
    cur = datetime.datetime(last_metric.time.year, last_metric.time.month,
                            last_metric.time.day, last_metric.time.hour)
    # cur = datetime.datetime(2020, 12, 14, 5)
    now_time = datetime.datetime.now()
    metrics = []

    while cur < now_time:
        next_time = cur + datetime.timedelta(hours=1)

        visits = session.query(Visit).filter(
            Visit.createdDate >= cur, Visit.createdDate < next_time).count()
        likes = session.query(Visit).filter(
            Visit.createdDate >= cur, Visit.createdDate < next_time, Visit.liked == True).count()
        users = session.query(User).filter(
            User.createdDate >= cur, User.createdDate < next_time).count()

        metrics = metrics + [
            Metric(time=cur, description=Metric.HOURLY_NEW_VISITS, amount=visits),
            Metric(time=cur, description=Metric.HOURLY_LIKES, amount=likes),
            Metric(time=cur, description=Metric.HOURLY_NEW_USERS, amount=users)
        ]

        print(len(metrics))

        if len(metrics) > 100:
            session.add_all(metrics)
            session.commit()
            metrics = []

        print("Adding metrics for", cur)

        session.add_all(metrics)
        session.commit()

        cur = next_time

    session.add_all(metrics)
    session.commit()

    session.close()


def count_unique_users(n):
    """
    Count the number of unique users who have a visit in the last n days.
    """

    session = Session()

    now = datetime.datetime.now()
    last_n_days = now - datetime.timedelta(days=n)

    users = session.query(Visit).filter(
        Visit.createdDate >= last_n_days
    ).distinct(Visit.userId).count()

    session.close()

    return users


print(count_unique_users(10))

session = Session()

# Delete all metrics with a time in the last 10 days
last_n_days = datetime.datetime.now() - datetime.timedelta(days=10)
session.query(Metric).filter(Metric.time >= last_n_days).delete()
session.commit()

session.close()
