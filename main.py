import core
import io
import logging
import sys
import threading
import time
import pandas as pd
from threading import Event
from typing import Optional
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

import api.utils as utils
from api.models import *
from config import *

log_filename = "attachments_service.log"

for module in ['docserverlib', 'docserverlib.docs', 'elasticsearch', 'azure', 'azure.storage', 'azure.storage.blob', 'sqlalchemy']:
    logging.getLogger(module).setLevel(logging.ERROR)


THREAD_COUNT = LISTENER_THREAD_COUNT
GLOBAL_QUEUE = [ATTACHMENT_QUEUE]

funcmap = {
    "process_attachment_request": core.process_attachments,
    "process_b64_attachment_request": core.process_b64,
    "process_recrawl_request": core.process_recrawl_requests
}

app = FastAPI(title="AttachmentHandler", default_response_class=JSONResponse, root_path=ROOT_PREFIX)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.debug = True

minio_client=utils.minio_client

BASIC_PROCESSING_CANCEL_TOKEN: Optional[Event]
LONG_PROCESSING_LISTENER_CANCEL_TOKEN: Optional[Event]
RECRAWL_LISTENER_CANCEL_TOKEN: Optional[Event]
B64_LISTENER_CANCEL_TOKEN: Optional[Event]

@app.on_event('startup')
def startup():
    global BASIC_PROCESSING_CANCEL_TOKEN
    global LONG_PROCESSING_LISTENER_CANCEL_TOKEN
    global RECRAWL_LISTENER_CANCEL_TOKEN
    global B64_LISTENER_CANCEL_TOKEN
    BASIC_PROCESSING_CANCEL_TOKEN = run_basic_request_handler()
    RECRAWL_LISTENER_CANCEL_TOKEN = run_recrawl_processing()
    B64_LISTENER_CANCEL_TOKEN = run_b64_processing()
    if ENQUEUE_PROCESSING:
        LONG_PROCESSING_LISTENER_CANCEL_TOKEN = run_processing_resolver()


@app.on_event("shutdown")
def shutdown():
    global BASIC_PROCESSING_CANCEL_TOKEN
    global LONG_PROCESSING_LISTENER_CANCEL_TOKEN
    global RECRAWL_LISTENER_CANCEL_TOKEN
    global B64_LISTENER_CANCEL_TOKEN

    if BASIC_PROCESSING_CANCEL_TOKEN:
        BASIC_PROCESSING_CANCEL_TOKEN.set()
    
    if RECRAWL_LISTENER_CANCEL_TOKEN:
        RECRAWL_LISTENER_CANCEL_TOKEN.set()

    if B64_LISTENER_CANCEL_TOKEN:
        B64_LISTENER_CANCEL_TOKEN.set()

    if ENQUEUE_PROCESSING:
        if LONG_PROCESSING_LISTENER_CANCEL_TOKEN:
            LONG_PROCESSING_LISTENER_CANCEL_TOKEN.set()


def read_log_file(filename, limit):
    buffer_size = 8192
    file_size = os.stat(filename).st_size
    iter = 0
    rtrn_data = ""
    with open(filename) as f:
        if buffer_size > file_size:
            buffer_size = file_size - 1
        fetched_lines = []
        while True:
            iter += 1
            f.seek(file_size-buffer_size * iter)
            fetched_lines.extend(f.readlines())
            if (len(fetched_lines) >= limit) or (f.tell == 0):
                rtrn_data = ''.join(fetched_lines[-limit:])
                break
    return rtrn_data


@app.post('/attachments/upload',
          summary='Submit a request for an attachment to be processed',
          response_class=JSONResponse,
          status_code=200,
          tags=["attachments"]
          )
def process_attachment_request(req: AttachmentRequest):
    request_data=req.dict()
    links=request_data.get('Links')
    if not links:
        return JSONResponse(status_code=400, content={"message": "No links provided"})
    request_data['_reqfunc'] = process_attachment_request.__name__
    request_data['status'] = "enqueued"
    result = utils.add_new_urls(request_data=request_data)
    if result:
        if 'All URLs' in result:
            logging.info(f"Duplicate request: {result}")
            return JSONResponse(status_code=200, content=f"Duplicate request found: {result}")
        else:
            logging.info(f"Request ID: {result}")
            return JSONResponse(status_code=200, content=f"Urls added as request: {result}")
    else:
        return JSONResponse(status_code=500, content="Error in adding URLs, please try again later.")


@app.post('/attachments/upload_b64',
          summary='Submit a request for an attachment to be processed',
          response_class=JSONResponse,
          status_code=200,
          tags=["attachments"]
          )
def process_b64_attachment_request(req: B64Request):
    request_data=req.dict()
    links=request_data.get('encodedUrls')
    if not links:
        return JSONResponse(status_code=400, content={"message": "No links provided"})
    request_data['_reqfunc'] = process_b64_attachment_request.__name__
    request_data['status'] = "enqueued"
    result = utils.add_new_b64_urls(request_data=request_data)
    if result:
        if 'All URLs' in result:
            logging.info(f"Duplicate request: {result}")
            return JSONResponse(status_code=200, content=f"Duplicate request found: {result}")
        else:
            logging.info(f"Request ID: {result}")
            return JSONResponse(status_code=200, content=f"Urls added as request: {result}")
    else:
        return JSONResponse(status_code=500, content="Error in adding URLs, please try again later.")


@app.post('/recover_single_image',
          summary='Submit a request for an URL to be recrawled',
          response_class=JSONResponse,
          status_code=200,
          tags=["attachments"]
          )
def process_recrawl_request(req: crawlProfilePicModel):
    try:
        request_data=req.dict()
    except:
        request_data = req
    if (not request_data['profile_id']) and (not request_data['profile_pic_url']):
        return JSONResponse(status_code=400, content={"message": "No ID or link provided for recrawl request"})
    request_data['_reqfunc'] = process_recrawl_request.__name__
    request_data['status'] = "enqueued"
    result = utils.add_new_recrawl(request_data=request_data)
    if result:
        if 'already' in result:
            logging.info(f"Duplicate request: {result}")
            return JSONResponse(status_code=200, content=f"Duplicate request found: {result}")
        else:
            logging.info(f"Request ID: {result}")
            return JSONResponse(status_code=200, content=f"Url added as recrawl request: {result}")
    else:
        return JSONResponse(status_code=500, content="Error in adding URL, please try again later.")


@app.get('/recover_image/{entityId}',
        summary = 'Download the file',
        status_code = 301,
        tags = ['attachments']
    )
def recover_image(entityId: str):
    temp_url = ""
    try:
        resp = utils.get_temp_url(entityId)
        if 'temp_url' in resp:
            temp_url = resp['temp_url']
            payload = {
                "profile_id": resp['social_id'],
                "profile_pic_url": temp_url,
                "social_media": resp['social_media'],
                "metadata": {
                    "EntityCard": entityId,
                    "updateField": resp['url_field']
                }
            }
            process_recrawl_request(payload)
    except Exception as e:
        print(f"Error in setting temp url and recover request: {e}")
    if temp_url == "":
        return JSONResponse(status_code=404, content={"error": "File not found"})
    return JSONResponse(status_code=301, headers={"Location": temp_url}, content={"url": temp_url})


@app.get("/dl/{id}",summary = 'Download the file',
        status_code = 301,
        tags = ['attachments']
        )
def download_file_from_id(id: str):
    try:
        url = utils.get_cached_data(id)
    except Exception as e:
        logging.error(f"Exception in reading cache: {e}")
        url = get_url_from_id(id, for_cache=True)
    if not url:
        return JSONResponse(status_code=404, content={"error": "File not found"})
    return JSONResponse(status_code=301, headers={"Location": url}, content={"url": url})


@app.get("/dl/raw/{id}",summary = 'Download the file',
        status_code = 301,
        tags = ['attachments']
        )
def get_url_from_id(id: str, for_cache=False):
    url = utils.retrieve_fs_url(id)
    if not for_cache:
        if not url:
            return JSONResponse(status_code=404, content={"error": "Requested URL not found"})
        return JSONResponse(status_code=200, content={"URL": url})
    else:
        if not url:
            return ""
        return url


@app.get("/queue_statistics", summary="Get basic statistics from table", status_code=200, tags=['additional'])
def get_stats():
    stats = utils.get_coll_stats()
    return JSONResponse(status_code=200, content=stats)


@app.get("/queue_statistics_new", summary="Get status counts from table", status_code=200, tags=['additional'])
def get_stats_new():
    stats = utils.get_individual_coll_stats()
    return JSONResponse(status_code=200, content=stats)


@app.get("/view_logs", summary="Get logs", status_code=200, tags=['additional'])
def get_logs():
    try:
        data = read_log_file(filename=f"logs/{log_filename}", limit=1500)
    except Exception as e:
        logging.error(f"Error in reading logs data: {e}")
        data = ""
    return StreamingResponse(content=io.StringIO(data), media_type="text/plain")


@app.get("/request_id_status/{req_id}", summary="Get status for each URL of a request ID", status_code=200, tags=['additional'])
def get_req_stats(req_id: str):
    stats = utils.get_request_id_status(req_id)
    return JSONResponse(status_code=200, content=stats)

@app.get('/dl/m/{file:path}',
         summary = 'Download File',
         tags = ['attachments'])
def download_file(file: str, response: Response):
    file=file.split('/')
    bucket=file[0]
    object='/'.join(file[1:])
    try:
        file_stream=minio_client.get_object(bucket, object).read()
    except Exception as e:
        response.status_code = 404
        return JSONResponse({"error": str(e)})
    response.headers["Content-Disposition"] = f"attachment; filename={object}"
    response.headers["Content-Type"] = "application/octet-stream"
    return Response(content=file_stream, media_type="application/octet-stream")


def count_basic_worker_threads() -> int:
    return sum(map(lambda item: item.name.startswith('process_worker'), threading.enumerate()))


def count_recrawl_worker_threads() -> int:
    return sum(map(lambda item: item.name.startswith('process_recrawl_worker'), threading.enumerate()))


def run_basic_request_handler() -> threading.Event:
    cancel_token = threading.Event()
    threading.Thread(target=listener, args=(cancel_token,)).start()
    return cancel_token


def run_processing_resolver() -> threading.Event:
    cancel_token = threading.Event()
    threading.Thread(target=processing_resolver, args=(cancel_token,)).start()
    return cancel_token


def run_b64_processing() -> threading.Event:
    cancel_token = threading.Event()
    threading.Thread(target=b64_listener, args=(cancel_token,)).start()
    return cancel_token


def run_recrawl_processing() -> threading.Event:
    cancel_token = threading.Event()
    threading.Thread(target=process_recrawl, args=(cancel_token,)).start()
    return cancel_token


def listener(cancel_token: threading.Event):
    while not cancel_token.is_set():
        try:
            # Ignore if all processes are running already
            worker_threads = count_basic_worker_threads()
            logging.debug(f'Basic worker threads: {worker_threads}')

            if worker_threads < THREAD_COUNT:
                # check for open requests in the following priority order
                df = None
                for q in GLOBAL_QUEUE:
                    # Find 'enqueued' URLs and set them to 'processing'
                    df = utils.get_enqueued_to_processing(limit=THREAD_COUNT)

                    if df.empty:
                        # logging.info(f"[LISTENER] found '0' enqueued rows for processing!")
                        break

                if not df.empty:
                    logging.info(f"[LISTENER] found '{len(df)}' enqueued rows for processing!")
                    df.set_index(['_id'], inplace=True, drop=True)
                    for idx, row in df.iterrows():
                        if row['reqfunc'] in funcmap.keys():
                            x = funcmap[row['reqfunc']]
                            orig_url = row['original_url']
                            metadata = row["metadata"]
                            threading.Thread(target=x, args=(idx, orig_url, metadata,), name=f'process_worker_{worker_threads}').start()
                        else:
                            logging.error("request function does not exist in the funcmap")
                            utils.update_status(status='error', url_id=idx)
            else:
                logging.error(f'There are no available worker threads; current count: {worker_threads}')
                logging.info(f'Sleeping for 5 seconds')
                time.sleep(5)
        except:
            logging.error(sys.exc_info()[1])
        finally:
            time.sleep(LISTENER_SLEEP_TIME)


def b64_listener(cancel_token: threading.Event):
    while not cancel_token.is_set():
        try:
            # Ignore if all processes are running already
            worker_threads = count_basic_worker_threads()
            logging.debug(f'Basic worker threads: {worker_threads}')

            if worker_threads < THREAD_COUNT:
                # check for open requests in the following priority order
                df = None
                for q in GLOBAL_QUEUE:
                    # Find 'enqueued' URLs and set them to 'processing'
                    df = utils.get_b64_to_processing(limit=THREAD_COUNT)

                    if df.empty:
                        # logging.info(f"[LISTENER] found '0' enqueued rows for processing!")
                        break

                if not df.empty:
                    logging.info(f"[LISTENER] found '{len(df)}' enqueued rows for b64 processing!")
                    df.set_index(['_id'], inplace=True, drop=True)
                    for idx, row in df.iterrows():
                        if row['reqfunc'] in funcmap.keys():
                            x = funcmap[row['reqfunc']]
                            orig_url = row['original_url']
                            b64_str = row['b64_str']
                            metadata = row["metadata"]
                            threading.Thread(target=x, args=(idx, orig_url,b64_str,metadata,), name=f'process_worker_{worker_threads}').start()
                        else:
                            logging.error("request function does not exist in the funcmap")
                            utils.update_status(status='error', url_id=idx)
            else:
                logging.error(f'There are no available worker threads; current count: {worker_threads}')
                logging.info(f'Sleeping for 5 seconds')
                time.sleep(5)
        except:
            logging.error(sys.exc_info()[1])
        finally:
            time.sleep(LISTENER_SLEEP_TIME)


def process_recrawl(cancel_token: threading.Event):
    while not cancel_token.is_set():
        try:
            # Ignore if all processes are running already
            worker_threads = count_recrawl_worker_threads()
            logging.debug(f'Basic worker threads: {worker_threads}')

            if worker_threads < THREAD_COUNT:
                # check for open requests in the following priority order
                df = None
                for q in GLOBAL_QUEUE:
                    # Find 'enqueued' URLs and set them to 'processing'
                    df = utils.get_recrawl_to_processing(limit=THREAD_COUNT)

                    if df.empty:
                        # logging.info(f"[LISTENER] found '0' enqueued recrawl requests for processing!")
                        break

                if not df.empty:
                    logging.info(f"[LISTENER] found '{len(df)}' recrawl requests for processing!")
                    df.set_index(['_id'], inplace=True, drop=True)
                    for idx, row in df.iterrows():
                        if row['reqfunc'] in funcmap.keys():
                            x = funcmap[row['reqfunc']]
                            orig_url = row['original_url']
                            metadata = row['metadata']
                            threading.Thread(target=x, args=(idx, orig_url, metadata,), name=f'process_recrawl_worker_{worker_threads}').start()
                        else:
                            logging.error("request function does not exist in the funcmap")
                            utils.update_status(status='error', url_id=idx)
            else:
                logging.error(f'There are no available worker threads; current count: {worker_threads}')
                logging.info(f'Sleeping for 5 seconds')
                time.sleep(5)
        except:
            logging.error(sys.exc_info()[1])
        finally:
            time.sleep(LISTENER_SLEEP_TIME)


def processing_resolver(cancel_token: threading.Event):
    while not cancel_token.is_set():
        try:
            worker_threads = count_basic_worker_threads()
            logging.debug(f'Basic worker threads: {worker_threads}')

            if worker_threads < THREAD_COUNT:
                utils.update_long_processing_to_enqueued()
        except:
            logging.error(sys.exc_info()[1])
        finally:
            time.sleep(LISTENER_SLEEP_TIME*90)
