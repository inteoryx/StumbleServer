"""
This script is intended to migrate from the old sqlite StumbleServer db to the 2020 model.
"""

import sqlite3, json, pony
from sqlalchemy.orm import sessionmaker
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from SqlAlchemyTables import *

#db_string = "postgresql://doadmin:sy1fhjbzc7lg4iu1@db-postgresql-nyc1-51092-do-user-8299087-0.b.db.ondigitalocean.com:25060/Stumblingon?sslmode=require"
#engine = create_engine(db_string, echo=False)
engine = create_engine("sqlite:////home/hapax/projects/StumbleServer/test.db", echo=False)

Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

conn = sqlite3.connect("OldStumble.db")
cur = conn.cursor()
"""
Copy over sites.
"""
cur.execute("select * from sites;")
rows = cur.fetchall()
unique_sites = {}
for row in rows:
    name = row[1].strip()
    unique_sites[name] = row

session = Session()
u = User(id="0")
session.add(u)
for row in unique_sites:
    siteId, url = unique_sites[row]
    session.add(Site(id=siteId.strip(), url=url.strip()))
    session.add(Submission(userId=u.id, url=url.strip(), status=1, reason="Imported in db migration."))
session.commit()

cur.execute("select * from users;")
rows = cur.fetchall()

#including_count = 0
#excluding_count = 0
count = 0
good_count = 0

session = Session()
for i, row in enumerate(rows):
    userId, j = row
    j = j.replace("\n", "")
    j = json.loads(j)

    if type(j) == type({}):
        user = User(id=userId)
        session.add(user)
        for k in j:
            try:
                s = session.query(Site).get(k)
                if not s:
                    continue
                session.add(Visit(siteId=k, userId=user.id, liked=j[k].get("liked", False)))
                good_count += 1
            except:
                count += 1

    if i % 10 == 0:
        print(f"{i}/{len(rows)}")
        session.commit()

session.commit()
print(f"Errors: {count} / {good_count + count}")
