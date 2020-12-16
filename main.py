from typing import Optional
from pydantic import BaseModel
from fastapi import FastAPI
import datetime, random
from multiprocessing import Lock
from uuid import uuid4
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

import os

from sqlalchemy.orm import sessionmaker
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from SqlAlchemyTables import *

import requests, time, threading

class Helper:
    busy = Lock()
    all_sites = set()
    secret = os.environ["SECRET_KEY"]
    date_format = "%Y-%m-%dT%h:%m:%s"

    def __init__(self, sm):
        self.sm = sm
        self.worker = threading.Thread(target=self.update, daemon=True)
        self.worker.start()
        self.metrics_file = open("metrics.html").read()

    def update(self):
        while True:
            session = self.sm()
            sites = session.query(Site).all()
            up_sites = []
            for site in sites:
                try:
                    r = requests.get(site.url, allow_redirects=True)
                    if r.status_code < 400:
                        up_sites.append(r)
                except:
                    pass
            print(f"{datetime.datetime.utcnow()}: {len(up_sites)} / {len(sites)} sites up")
            self.all_sites = up_sites
            time.sleep(86400) #Run this once per day.

            
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db_string = os.environ["DB_STRING"]
engine = create_engine(db_string, echo=False, pool_size=15)
Session = sessionmaker(bind=engine)

session = Session()
helper = Helper(Session)
helper.all_sites = {site.id:site for site in session.query(Site).all()}

class GetUserRequest(BaseModel):
    userId: str

class GetSiteRequest(BaseModel):
    userId: str
    prevId: str = ""

class GetHistoryRequest(BaseModel):
    userId: str
    start: int
    pageSize: int = 10

class SubmitSiteRequest(BaseModel):
    userId: str
    url: str

class LikeRequest(BaseModel):
    userId: str
    siteId: str

class AddSiteRequest(BaseModel):
    auth: str
    url: str
    userId: str
    reason: Optional[str]

class UpdateSubmissionsRequest(BaseModel):
    auth: str
    url: str = "http://example.com"
    newStatus: int
    reason: Optional[str]

class GetSubmissionsRequest(BaseModel):
    status: int

class HistoryStumblesRequest(BaseModel):
    start: str = "2020-12-01T00:00:00"
    end: str = "2020-12-01T00:00:00"
    increment: int = 60
    liked: bool = False
    
def format_date(d: str):
    try:
        return datetime.datetime.strptime(d, "%Y-%m-%dT%H:%M:%S")
    except:
        return None

def get_site_result(url: str, site_id: str):
    return {
        'url': url,
        'siteId': site_id,
        "ok": True
    }

def submit_site_result(message: str):
    return {
        "message": message,
        "ok": True
    }

def get_history_result(history, more):
    return {
        "size": len(history),
        "more": more,
        "results": history,
        "ok": True
    }

def visits_between(start, end, like_required):
    result = []
    session = Session()
    result.append((start, session.query(Visit).filter(
        Visit.createdDate.between(start, end),
        (not like_required or Visit.liked)).count()
    ))

    return result

def users_between(start, end):
    result = []
    session = Session()
    result.append((start, len(set(s.userId for s in session.query(Visit).filter(Visit.createdDate.between(start, end)).all())) ))

    return result

def get_user(user_id):
    session = Session()
    user = session.query(User).get(user_id)
    if not user:
        user = User(id=user_id)
        session.add(user)
        session.commit()
    return user

@app.get("/metrics", response_class=HTMLResponse)
def read_root():
    return HTMLResponse(content=helper.metrics_file, status_code=200)

@app.post("/getSite")
async def get_site(r: GetSiteRequest):
    session = Session()
    user = get_user(r.userId)
    prev_site = None

    if r.prevId:
        prev_site = session.query(Site).get(r.prevId)

    if prev_site and (not session.query(Visit).get((prev_site.id, user.id))):
        session.add(Visit(userId=user.id, siteId=prev_site.id))
        session.commit()

    visited = set(h.siteId for h in session.query(Visit).filter_by(userId=user.id).all())
    choose_from = tuple(helper.all_sites.keys() - visited)

    if len(choose_from) < 1:
        return {"message": "We have no further sites you haven't visited.  Please come back later.  We may get more.", "ok": False}

    result = helper.all_sites[random.choice(choose_from)]
    session.close()
    return get_site_result(result.url, result.id)

@app.post("/submitSite")
async def submit_site(r: SubmitSiteRequest):
    user = get_user(r.userId)
    session = Session()
    prev_sub = session.query(Submission).get(r.url)
    if prev_sub:
        return submit_site_result(f"{r.url} has already been submitted and status is {prev_sub.get_status()}")
    else:
        session.add(Submission(url=r.url, userId=user.id))

    session.commit()
    session.close()
    return submit_site_result("Thanks for your submission.  It will be reviewed, and, if approved, added to our index.")

@app.post("/getHistory")
async def get_history(r: GetHistoryRequest):
    user = get_user(r.userId)
    visits = None
    session = Session()
    visits = session.query(Visit).filter_by(userId=user.id).order_by(Visit.createdDate.desc()).offset(r.start).limit(r.pageSize+1).all()
    visits = [{"id": v.siteId, "url": v.site.url, "liked": v.liked, "visitDate": v.createdDate} if v.site != None else {"id": "error", "url": "this site has been removed from the index", "liked": v.liked} for v in visits ]
    session.close()
    #We get 1 more than the page size and cut it off.  This tells us if more results exist.
    return get_history_result(visits[:r.pageSize], len(visits) > r.pageSize)

@app.post("/like")
async def like(r: LikeRequest):
    user = get_user(r.userId)
    session = Session()
    visit = session.query(Visit).get((r.siteId, user.id))

    if not visit:
        return {"error": True, "message": f"User {r.userId} has not visited {r.siteId}", "ok": False}

    final_like_state = not visit.liked
    visit.liked = final_like_state
    session.commit()
    session.close()
    return {"liked": final_like_state, 'ok': True}

@app.post("/updateSubmissions")
async def update_submissions(r: UpdateSubmissionsRequest):
    if r.auth != helper.secret:
        return {
            'errorMessage': "Invalid auth key",
            'friendlyMessage': "I do not recognize your secret password.",
            'isRetryable': False,
            'ok': False
    }

    session = Session()
    submission = session.query(Submission).get(r.url)
    if not submission:
        return {"ok": False, "message": f"There was not submission for {r.url}"}

    submission.status = r.newStatus
    submission.reason = r.reason

    session.commit()
    session.close()

    return {"ok": True, "submissionId": submission.url}

@app.post("/getSubmissions")
async def get_submissions(r: GetSubmissionsRequest):
    results = None
    session = Session()
    results = [s.j() for s in (session.query(Submission).filter_by(status=r.status))]
    session.close()
    return {"submissions": results, "size": len(results)}

@app.post("/addSite")
async def like(r: AddSiteRequest):
    if r.auth != helper.secret:
        return {
            'errorMessage': "Invalid auth key",
            'friendlyMessage': "I do not recognize your secret password.",
            'isRetryable': False,
            'ok': False
    }

    new_site = None
    session = Session()
    submission = session.query(Submission).get(r.url)
    if not submission:
        submission = Submission(
            userId=get_user(r.userId).id, 
            status=1,
            reason="Submitted by owner.",
            url=r.url
        )
        session.add(submission)
    else:
        submission.status = 1
        submission.reason = r.reason if r.reason else ""

    new_site = Site(id=str(uuid4()), url=submission.url)
    session.add(new_site)
    session.commit()
    session.close()
    return {"ok": True, "siteId": new_site.id}

@app.post("/historyStumbles")
async def history_stumbles(r: HistoryStumblesRequest):
    """
    Intended to return number of stumbles over time
    """
    start = format_date(r.start)
    end = format_date(r.end)
    increment = datetime.timedelta(minutes=r.increment)

    if not start or not end:
        return {"message": "start and end need format: 'YYYY-MM-DDTHH:MM:SS'"}

    result = []
    while start < end:
        result.extend(visits_between(start, start+increment, like_required=r.liked))
        start = start + increment
    session.close()
    return result

@app.post("/historyUsers")
async def history_stumbles(r: HistoryStumblesRequest):
    """
    Intended to return number of unique users over time
    """
    start = format_date(r.start)
    end = format_date(r.end)
    increment = datetime.timedelta(minutes=r.increment)

    if not start or not end:
        return {"message": "start and end need format: 'YYYY-MM-DDTHH:MM:SS'"}

    result = []
    while start < end:
        result.extend(users_between(start, start+increment))
        start = start + increment
    session.close()        
    return result
