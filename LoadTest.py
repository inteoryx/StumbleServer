import requests, time, asyncio
from concurrent.futures import ThreadPoolExecutor, wait
from uuid import uuid4


def get_site(uid, n):
    prevId = ""
    visited = set()
    for _ in range(n):
        resp = requests.post("http://127.0.0.1:8000/getSite", json={"userId": uid, "prevId": prevId})
        j = resp.json()
        if j["siteId"] in visited:
            print(f"Repeat on {j['siteId']}")
        visited.add(j["siteId"])
        #print(j["siteId"], prevId)
        prevId = resp.json()["siteId"]
    return resp

N = 16
while N < 128:
    result = []
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=40) as tpe:
        futures = [tpe.submit(get_site, f"loadtest{str(uuid4())}", 100) for _ in range(N)]
        
        done, not_done = wait(futures, timeout=None)

        print(len(done), len(not_done))

    print(f"{time.time() - start_time}, {N * 100}")
    N = N * 2
