import aiohttp
import async_timeout
import hashlib
import logging
import mimetypes
import pandas as pd
import requests
import time
import os
import redis
from azure.storage.blob import BlobServiceClient, ContainerClient, ContentSettings, generate_blob_sas
from azure.storage.blob._models import BlobSasPermissions
from docserverlib import DocserverClient
from datetime import datetime, timedelta
import pymongo
from config import *
import minio
import moviepy.editor as mp

for module in ['docserverlib', 'docserverlib.docs', 'elasticsearch', 'azure', 'azure.storage', 'azure.storage.blob', 'sqlalchemy']:
    logging.getLogger(module).setLevel(logging.ERROR)


def get_ds_client():
    if MINIO_SECURE:
        secure = True
    else:
        secure = False
    ds = DocserverClient(
        es_url=ES_URL,
        es_user=ES_USER,
        es_password=ES_PASSWORD,
        minio_url=MINIO_URL,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        minio_secure=secure)
    return ds

if MINIO_SECURE:
    minio_sec = True
else:
    minio_sec = False
minio_client=minio.Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=minio_sec)

ds_client = get_ds_client()


def create_mongo_client():
    mngClnt = pymongo.MongoClient(MONGO_CONNECTION_STRING)
    mongoDB = mngClnt[DB_NAME]
    mongoColl = mongoDB[URLS_COLLECTION]
    mongoRecrawlColl = mongoDB[RECRAWL_COLLECTION]
    mongoRecoverColl = mongoDB[RECOVER_COLLECTION]
    mongoB64Coll = mongoDB[B64_COLLECTION]

    return mngClnt, mongoDB, mongoColl, mongoRecrawlColl, mongoRecoverColl, mongoB64Coll


mngClnt, mongoDb, mongoColl, mongoRecrawlColl, mongoRecoverCrawl, mongoB64Coll = create_mongo_client()


def redis_conn():
    try:
        rd_clnt = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            db=REDIS_DB,
            socket_timeout=5
        )
        ping = rd_clnt.ping()
        if ping:
            return rd_clnt
    except redis.AuthenticationError:
        logging.error('Redis: Authentication Error')


redis_client = redis_conn()


def create_azure_clients():
    az_service_clnt = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container = ContainerClient.from_connection_string(AZURE_CONNECTION_STRING, AZURE_CONTAINER_NAME)
    if container.exists():
        az_cont_clnt = az_service_clnt.get_container_client(AZURE_CONTAINER_NAME)
    else:
        az_cont_clnt = az_service_clnt.create_container(AZURE_CONTAINER_NAME)

    return az_service_clnt, az_cont_clnt


az_service_clnt, az_cont_clnt = create_azure_clients()


def get_azure_token(filename):
    expiry = datetime.utcnow() + timedelta(seconds=18600)
    az_acc_name = az_service_clnt.account_name
    az_acc_key = az_service_clnt.credential.account_key

    sas_blob = generate_blob_sas(account_name=az_acc_name, container_name=AZURE_CONTAINER_NAME, blob_name=filename, account_key=az_acc_key, permission=BlobSasPermissions(read=True), expiry=expiry)

    return sas_blob


def upload_to_azure_blob_with_copy(filename, url):
    blob_properties = {}
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        copied_blob = blob_service_client.get_blob_client(AZURE_CONTAINER_NAME, filename)
        copy_stats = copied_blob.start_copy_from_url(url)
        copy_properties = copied_blob.get_blob_properties()
        while copy_properties.copy.status == "pending":
            time.sleep(0.5)
            copy_properties = copied_blob.get_blob_properties()
        
        if copy_properties.copy.status == "failed":
            return str(copy_properties.copy.status_description)

        az_acc_name = az_service_clnt.account_name

        sas_blob = get_azure_token(filename)

        blob_url = f"https://{az_acc_name}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/{filename}"
        blob_properties['fileUrl'] = blob_url
        blob_properties['token'] = sas_blob
        blob_properties['fileName'] = filename
        blob_properties['fileSize'] = copy_properties.size
        return blob_properties
    except Exception as e:
        return str(e)


def upload_to_azure_blob(filename, content_type, local_filepath):
    blob_properties = {}
    try:
        az_blob_clnt = az_service_clnt.get_blob_client(container=AZURE_CONTAINER_NAME, blob=filename)

        with open(local_filepath, "rb") as data:
            content_type = ContentSettings(content_type=content_type)
            az_blob_clnt.upload_blob(data, content_settings=content_type, overwrite=True)

        az_acc_name = az_service_clnt.account_name
        sas_blob = get_azure_token(filename)

        blob_url = f"https://{az_acc_name}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/{filename}"
        blob_properties['fileUrl'] = blob_url
        blob_properties['token'] = sas_blob
        blob_properties['fileName'] = filename
        return blob_properties
    except Exception as e:
        return str(e)


def delete_blob(url_id):
    try:
        az_cont_clnt.delete_blob(url_id)
        logging.info(f"Blob '{url_id}' deleted successfully.")
    except Exception as e:
        logging.exception(f"Exception in deleting the blob: {e}")


def check_priority(url):
    priority = 1
    if ("fbcdn" in url) or ("facebook" in url) or ("cdninstagram" in url):
        priority = 2
    return priority


def add_new_urls(request_data: dict):
    try:
        first_create_time = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        req_id = hashlib.md5(datetime.strftime(datetime.utcnow(), "%Y%m%dT%H%M%S.%fZ").encode("utf-8")).hexdigest()
        insert_data = []
        request_data['Links'] = list(set(request_data['Links']))
        for link in request_data['Links']:
            priority = check_priority(link)
            if request_data['hash_input']:
                if priority == 2:
                    orig_file_name = link.split("/")[-1].split("?")[0]
                    url_id = hashlib.md5(orig_file_name.encode('utf-8')).hexdigest()
                else:
                    url_id = hashlib.md5(link.encode('utf-8')).hexdigest()
            else:
                url_id = link.split('/')[-1].split('.')[0]
            insert_data.append({"_id": url_id, "req_id": req_id, "url_id": url_id, "original_url": link, "priority": priority, "status": request_data['status'], "reqfunc": request_data['_reqfunc'], "created_date": first_create_time, "modified_date": first_create_time, "metadata": request_data['metadata']})
        try:
            insert_stats = mongoColl.insert_many(insert_data, ordered=False)
            if insert_stats:
                if len(insert_stats.inserted_ids) > 0:
                    return req_id
        except Exception as ex:
            if ex.details['nInserted'] == 0:
                return "All URLs are already indexed!"
            else:
                return req_id
    except Exception as e:
        logging.exception(f"Exception in adding URLs for request: {e}")
    return False


def add_new_b64_urls(request_data: list):
    try:
        first_create_time = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        req_id = hashlib.md5(datetime.strftime(datetime.utcnow(), "%Y%m%dT%H%M%S.%fZ").encode("utf-8")).hexdigest()
        insert_data = []
        for url_set in request_data['encodedUrls']:
            link = url_set['url']
            b64_str = url_set['b64_str']
            url_id = hashlib.md5(link.encode("utf-8")).hexdigest()
            insert_data.append({"_id": url_id, "req_id": req_id, "url_id": url_id, "original_url": link, "b64_str": b64_str, "status": request_data['status'], "reqfunc": request_data['_reqfunc'], "created_date": first_create_time, "modified_date": first_create_time, "metadata": request_data['metadata']})
        try:
            insert_stats = mongoColl.insert_many(insert_data, ordered=False)
            if insert_stats:
                if len(insert_stats.inserted_ids) > 0:
                    return req_id
        except Exception as ex:
            if ex.details['nInserted'] == 0:
                return "All URLs are already indexed!"
            else:
                return req_id
    except Exception as e:
        logging.exception(f"Exception in adding URLs for request: {e}")
    return False 


def add_new_recrawl(request_data: dict):
    try:

        profile_url = ""
        if request_data['social_media'] == 'facebook':
            if request_data['profile_id']:
                profile_url = f"https://graph.facebook.com/{request_data['profile_id']}/picture?height=500&access_token={FB_ACCESS_TOKEN}"
        else:
            if not request_data['profile_pic_url']:
                return False
            profile_url = request_data['profile_pic_url']

        first_create_time = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        req_id = hashlib.md5(datetime.strftime(datetime.utcnow(), "%Y%m%dT%H%M%S.%fZ").encode("utf-8")).hexdigest()
        url_id = hashlib.md5(profile_url.encode('utf-8')).hexdigest()
        insert_data = {"_id": url_id, "req_id": req_id, "url_id": url_id, "original_url": profile_url, "priority": check_priority(profile_url), "status": request_data['status'], "reqfunc": request_data['_reqfunc'], "created_date": first_create_time, "modified_date": first_create_time, "metadata": request_data['metadata']}
        try:
            insert_stats = mongoColl.insert_one(insert_data)
            if insert_stats:
                if insert_stats.inserted_id:
                    return req_id
        except Exception as ex:
            if ex.details['code'] == 11000:
                reset_status(url_id=url_id)
                return "Request already indexed!"
            else:
                return req_id
    except Exception as e:
        logging.exception(f"Exception in adding URL for recrawl request: {e}")
    return False


def get_enqueued_to_processing(status="enqueued", reqFunc="process_attachment_request", limit=1):
    rtrn_list = []
    try:
        if SORT_ENQUEUED_BY_LATEST:
            sort_order = [("created_date", pymongo.DESCENDING), ("priority", pymongo.DESCENDING)]
        else:
            sort_order = [("created_date", pymongo.ASCENDING), ("priority", pymongo.DESCENDING)]
        data = mongoColl.find({"status": status, "reqfunc": reqFunc}, {"original_url": 1, "reqfunc": 1, "metadata": 1}).sort(sort_order).limit(limit)
        rows = [itm for itm in data]
        if len(rows) > 0:
            ids = [itm['_id'] for itm in rows]
            cur_datetime = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            updt_ok = mongoColl.update_many(filter={"_id": {"$in": ids}}, update={"$set": {"status": "processing", "modified_date": cur_datetime}})
            if updt_ok:
                if updt_ok.raw_result['nModified'] > 0:
                    rtrn_list = rows
    except Exception as e:
        logging.exception(f"Exception in getting enqueued URLs: {e}")
    return pd.DataFrame(rtrn_list)


def get_recrawl_to_processing(status="enqueued", reqFunc="process_recrawl_request", limit=1):
    rtrn_list = []
    try:
        if SORT_ENQUEUED_BY_LATEST:
            sort_order = [("created_date", pymongo.DESCENDING), ("priority", pymongo.DESCENDING)]
        else:
            sort_order = [("created_date", pymongo.ASCENDING), ("priority", pymongo.DESCENDING)]
        data = mongoColl.find({"status": status, "reqfunc": reqFunc}, {"original_url": 1, "reqfunc": 1, "metadata": 1}).sort(sort_order).limit(limit)
        rows = [itm for itm in data]
        if len(rows) > 0:
            ids = [itm['_id'] for itm in rows]
            cur_datetime = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            updt_ok = mongoColl.update_many(filter={"_id": {"$in": ids}}, update={"$set": {"status": "processing", "modified_date": cur_datetime}})
            if updt_ok:
                if updt_ok.raw_result['nModified'] > 0:
                    rtrn_list = rows
    except Exception as e:
        logging.exception(f"Exception in getting enqueued URLs: {e}")
    return pd.DataFrame(rtrn_list)


def get_b64_to_processing(status="enqueued", reqFunc="process_b64_attachment_request", limit=1):
    rtrn_list = []
    try:
        if SORT_ENQUEUED_BY_LATEST:
            sort_order = [("created_date", pymongo.DESCENDING), ("priority", pymongo.DESCENDING)]
        else:
            sort_order = [("created_date", pymongo.ASCENDING), ("priority", pymongo.DESCENDING)]
        data = mongoColl.find({"status": status, "reqfunc": reqFunc}, {"original_url": 1, "b64_str": 1, "reqfunc": 1, "metadata": 1}).sort(sort_order).limit(limit)
        rows = [itm for itm in data]
        if len(rows) > 0:
            ids = [itm['_id'] for itm in rows]
            cur_datetime = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            updt_ok = mongoColl.update_many(filter={"_id": {"$in": ids}}, update={"$set": {"status": "processing", "modified_date": cur_datetime}})
            if updt_ok:
                if updt_ok.raw_result['nModified'] > 0:
                    rtrn_list = rows
    except Exception as e:
        logging.exception(f"Exception in getting enqueued URLs: {e}")
    return pd.DataFrame(rtrn_list)


def update_long_processing_to_enqueued(status="processing"):
    rtrn_list = []
    try:
        cur_datetime = datetime.strftime(datetime.utcnow() - timedelta(hours=PROCESSING_CHECK_HOURS), "%Y-%m-%dT%H:%M:%SZ")
        data = mongoColl.find({"status": status, "modified_date": {"$lt": cur_datetime}}, {"original_url": 1})
        rows = [itm for itm in data]
        if len(rows) > 0:
            ids = [itm['_id'] for itm in rows]
            cur_datetime = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            updt_ok = mongoColl.update_many(filter={"_id": {"$in": ids}}, update={"$set": {"status": "enqueued", "created_date": cur_datetime, "modified_date": cur_datetime}})
            if updt_ok:
                if updt_ok.raw_result['nModified'] > 0:
                    rtrn_list = rows
    except Exception as e:
        logging.exception(f"Exception in updating long processing URLs: {e}")
    return pd.DataFrame(rtrn_list)


def add_fs_url(fs_url, url_id):
    try:
        cur_datetime = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        updt_ok = mongoColl.update_one(filter={"_id": url_id}, update={"$set": {"status": "success", "fs_url": fs_url, "modified_date": cur_datetime}})
        if updt_ok:
            if updt_ok.raw_result['nModified'] > 0:
                return True
    except Exception as e:
        logging.error(f"Exception while updating FS Url: {e}")
    return False


def add_recrawl_fs_url(fs_url, url_id):
    try:
        cur_datetime = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        updt_ok = mongoRecrawlColl.update_one(filter={"_id": url_id}, update={"$set": {"status": "success", "fs_url": fs_url, "modified_date": cur_datetime}})
        if updt_ok:
            if updt_ok.raw_result['nModified'] > 0:
                return True
    except Exception as e:
        logging.error(f"Exception while updating recrawl FS Url: {e}")
    return False


def reset_status(url_id):
    try:
        cur_datetime = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        updt_ok = mongoColl.update_one(filter={"_id": url_id}, update={"$set": {"status": "enqueued", "modified_date": cur_datetime}})
        if updt_ok:
            if updt_ok.raw_result['nModified'] > 0:
                return True
    except Exception as e:
        logging.error(f"Exception while updating Status: {e}")
    return False


def update_status(status, url_id):
    try:
        cur_datetime = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        updt_ok = mongoColl.update_one(filter={"_id": url_id}, update={"$set": {"status": status, "modified_date": cur_datetime}})
        if updt_ok:
            if updt_ok.raw_result['nModified'] > 0:
                return True
    except Exception as e:
        logging.error(f"Exception while updating Status: {e}")
    return False


def update_recrawl_status(status, url_id):
    try:
        cur_datetime = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        updt_ok = mongoRecrawlColl.update_one(filter={"_id": url_id}, update={"$set": {"status": status, "modified_date": cur_datetime}})
        if updt_ok:
            if updt_ok.raw_result['nModified'] > 0:
                return True
    except Exception as e:
        logging.error(f"Exception while updating Status: {e}")
    return False


def get_routes_from_cache(key: str) -> str:
    val = redis_client.get(key)
    return val


def set_routes_to_cache(key: str, val: str, caching_expiry: int) -> bool:
    state = redis_client.setex(key, timedelta(seconds=caching_expiry), value=val, )
    return state


def get_cached_data(id: str) -> str:
    data = get_routes_from_cache(key=id)
    if not data:
        data = retrieve_fs_url(url_id=id)
    else:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        logging.info(f"Found cached key: {id}->{data}")
    return data


def retrieve_fs_url(url_id):
    try:
        data = mongoColl.find({"_id": url_id}, {"original_url": 1, "fs_url": 1})
        row = [itm for itm in data]
        if len(row) > 0:
            if 'fs_url' in row[0]:
                rtrn_url = row[0]['fs_url']
                if AZURE_ACCOUNT_NAME in rtrn_url and MINIO_PROXY_MODE:
                    url="/".join(rtrn_url.split("/")[3:])
                    rtrn_url = f"{SERVER_BASE_URL}/dl/m/{url}"
                state = set_routes_to_cache(key=url_id, val=rtrn_url, caching_expiry=18000)
            else:
                rtrn_url = row[0]['original_url']
                state = set_routes_to_cache(key=url_id, val=rtrn_url, caching_expiry=3600)
            if state:
                logging.info(f"Data cached: {url_id}->{rtrn_url}")
            return rtrn_url
    except Exception as e:
        logging.exception(f"Exception in retrieving FS URL: {e}")
    return False


def get_coll_stats():
    try:
        stat_dicts = mongoColl.aggregate([{"$group": {"_id": "$status", "count": {"$sum": 1}}}])
        rows = [itm for itm in stat_dicts]
        if len(rows) > 0:
            stats = {}
            for row in rows:
                if row['_id']:
                    stats[row['_id']] = row['count']
            return stats
    except Exception as e:
        logging.exception(f"Exception in getting queue statistics: {e}")
    return {}


def get_individual_coll_stats():
    try:
        status_counts = {'enqueued': 0, 'processing': 0, 'success': 0, 'error': 0}
        for key, val in status_counts.items():
            curr_count = mongoColl.count_documents(filter={"status": key})
            status_counts[key] = curr_count
        return status_counts
    except Exception as e:
        logging.exception(f"Exception in getting queue statistics: {e}")
    return {}


def get_request_id_status(req_id):
    try:
        data = mongoColl.find({"req_id": req_id}, {"url_id": 1, "original_url": 1, "fs_url": 1, "status": 1})
        row = [itm for itm in data]
        if len(row) > 0:
            return row
    except Exception as e:
        logging.exception(f"Exception in getting request ID based status: {e}")
    return {}


async def downloader(session, url_id, url):
    os.makedirs('temp', exist_ok=True)
    retry = 3
    fs_url = False
    while retry:
        try:
            with async_timeout.timeout(120):
                async with session.get(url) as resp:
                    if resp.status != 200:
                        raise Exception("Response status not 200!")
                    else:
                        file_id = hashlib.md5(url.encode("utf-8")).hexdigest()
                        content_type = resp.headers["Content-Type"].replace('.', '').replace('jpg', 'jpeg')
                        file_ext = mimetypes.guess_extension(content_type)
                        filepath = os.path.join('temp', f'{file_id}{file_ext}')
                        filename = file_id
                        with open(filepath, 'wb') as f:
                            while True:
                                chunk = await resp.content.read(1024)
                                if not chunk:
                                    break
                                f.write(chunk)
                            f.close()
                        await resp.release()
                        try:
                            f = ds_client.files.upload_file(file_path=filepath, file_id=file_id)

                            if not f:
                                raise Exception(f"Failed to upload: {url}")

                        except Exception as e:
                            logging.error(f"[AioHttp] Upload failed! {url}, Exception - {e}")
                            continue

                        fs_url = FS_BASE_URL + f['fileUrl']
                        retry = 1
                        try:
                            os.remove(filepath)
                        except:
                            pass
        except Exception as e:
            logging.exception(f"Exception while downloading URL: {url} | Error: {e}")
        retry -= 1

    if fs_url:
        logging.info(f"Uploaded {url}")
        add_fs_url(fs_url=fs_url, url_id=url_id)
    else:
        update_status(status='error', url_id=url_id)


async def uploader_main(urls):
    os.makedirs('temp', exist_ok=True)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    async with aiohttp.ClientSession(headers=headers) as sess:
        for url_set in urls:
            for url_id, url in url_set.items():
                await downloader(sess, url_id, url)


async def recrawl_downloader(session, url_id, url):
    os.makedirs('temp', exist_ok=True)
    retry = 3
    fs_url = False
    while retry:
        try:
            with async_timeout.timeout(120):
                async with session.get(url) as resp:
                    if resp.status != 200:
                        raise Exception("Response status not 200!")
                    else:
                        file_id = hashlib.md5(url.encode("utf-8")).hexdigest()
                        content_type = resp.headers["Content-Type"].replace('.', '').replace('jpg', 'jpeg')
                        file_ext = mimetypes.guess_extension(content_type)
                        filepath = os.path.join('temp', f'{file_id}{file_ext}')
                        filename = file_id
                        with open(filepath, 'wb') as f:
                            while True:
                                chunk = await resp.content.read(1024)
                                if not chunk:
                                    break
                                f.write(chunk)
                            f.close()
                        await resp.release()
                        try:
                            f = ds_client.files.upload_file(file_path=filepath, file_id=file_id)

                            if not f:
                                raise Exception(f"Failed to upload: {url}")

                        except Exception as e:
                            logging.error(f"[AioHttp] Upload failed! {url}, Exception - {e}")
                            continue

                        fs_url = FS_BASE_URL + f['fileUrl']
                        retry = 1
                        try:
                            os.remove(filepath)
                        except:
                            pass
        except Exception as e:
            logging.exception(f"Exception while downloading URL: {url} | Error: {e}")
        retry -= 1

    if fs_url:
        logging.info(f"Uploaded {url}")
        add_recrawl_fs_url(fs_url=fs_url, url_id=url_id)
    else:
        update_recrawl_status(status='error', url_id=url_id)


async def recrawl_uploader_main(urls, proxy):
    os.makedirs('temp', exist_ok=True)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    async with aiohttp.ClientSession(headers=headers, proxy=proxy) as sess:
        for url_set in urls:
            for url_id, url in url_set.items():
                await recrawl_downloader(sess, url_id, url)


def download_file(url: str, proxy = None, fName=None, fPath=None) -> str:
    os.makedirs('temp', exist_ok=True)
    retry = 3
    content_type = False
    filename = fName
    filepath = fPath
    while retry:
        start = time.time()
        try:
            headers = {
                'Referer': url,
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
            }
            timeout = 90

            proxies = {}
            if proxy:
                proxies = {"http": proxy, "https": proxy}

            response = requests.get(url, stream=True, headers=headers, timeout=timeout, proxies=proxies)
            if response.status_code != 200:
                raise Exception(f"Failed to download: {url}")
            content_type = response.headers["Content-Type"].replace('.','').replace('jpg', 'jpeg')
            file_ext = mimetypes.guess_extension(content_type)
            if not filepath:
                filepath = os.path.join('temp', f'{hashlib.md5(url.encode("utf-8")).hexdigest()}{file_ext}')
            if not filename:
                filename = f'{hashlib.md5(url.encode("utf-8")).hexdigest()}'

            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if time.time() - start > timeout:
                        raise Exception(f"Download timeout for {url}")
                    if chunk:
                        f.write(chunk)
                f.close()
            retry = 1
        except Exception as e:
            logging.exception(f"Exception in downloading via requests for URL:{url} | Error: {e}")
        retry -= 1
    if not filepath:
        logging.error(f"Exhausted retries with requests downloader for URL: {url}")

    return content_type, filename, filepath


def upload_files(row):
    for key, val in row.items():
        fs_url = False
        f = False
        try:
            content_type, filename, filepath = download_file(val)
            if filepath:
                file_id = hashlib.md5(val.encode("utf-8")).hexdigest()
                f = ds_client.files.upload_file(file_path=filepath, file_id=file_id)
            if not f:
                raise Exception(f"Failed to upload: {val}")

            fs_url = FS_BASE_URL + f['fileUrl']
            try:
                os.remove(filepath)
            except:
                pass
        except Exception as e:
            logging.error(f"[Requests] Upload failed! {val}, Exception - {e}")

        if fs_url:
            logging.info(f"Uploaded {val}")
            add_fs_url(fs_url=fs_url, url_id=key)
        else:
            update_status(status='error', url_id=key)


def upload_recrawl_files(row):
    for key, val in row.items():
        fs_url = False
        f = False
        try:
            content_type, filename, filepath = download_file(val)
            if filepath:
                file_id = hashlib.md5(val.encode("utf-8")).hexdigest()
                f = ds_client.files.upload_file(file_path=filepath, file_id=file_id)
            if not f:
                raise Exception(f"Failed to upload: {val}")

            fs_url = FS_BASE_URL + f['fileUrl']
            try:
                os.remove(filepath)
            except:
                pass
        except Exception as e:
            logging.error(f"[Requests] Upload failed! {val}, Exception - {e}")

        if fs_url:
            logging.info(f"Uploaded {val}")
            add_recrawl_fs_url(fs_url=fs_url, url_id=key)
        else:
            update_recrawl_status(status='error', url_id=key)


def update_ec_url(ec_id, doc={}):
    try:
        resp = ds_client.docs.update_document(collection=ENTITY_CARD_COLLECTION, doc_id=ec_id, doc=doc, merge_array_fields=False)
    except Exception as e:
        resp = str(e)
        logging.error(f"Error while updating fs_url in Entity Card: {e}")
    return resp


def get_temp_url(entity_id, social_media="facebook", id_field="_facebookId", url_field="_facebookProfileImgUrlGlobal"):
    rtrn = {}
    try:
        resp = ds_client.docs.get_document(collection=ENTITY_CARD_COLLECTION, doc_id=entity_id, fields_to_return=f"socialNetworks,{id_field}")
        if id_field in resp:
            if social_media == "facebook":
                temp_url = f"https://graph.facebook.com/{resp[id_field]}/picture?height=500&access_token={FB_ACCESS_TOKEN}"
            else:
                temp_url = ""
            rtrn = {
                "temp_url": temp_url,
                "social_media": social_media,
                "social_id": resp[id_field],
                "url_field": url_field
            }
    except Exception as e:
        logging.error(f"Error in getting entity card: {e}")
    return rtrn


def get_status(mapping_id):
    try:
        data = mongoColl.find({"_id": mapping_id}, {"url_id": 1, "original_url": 1, "fs_url": 1, "status": 1, "metadata": 1})
        row = [itm for itm in data]
        if len(row) > 0:
            return row
    except Exception as e:
        logging.exception(f"Exception in getting ID based status: {e}")
    return {}

def upload_to_file_collection(file_info: dict):
    if file_info.get("_eventIds") or file_info.get("_entityIds"):
        fileUrl = "/" + '/'.join(file_info["file_url"].split("/")[3:])
        doc = {
            "_id": file_info.get("id"),
            "fileName": file_info.get("id"),
            "fileType": file_info.get("file_type"),
            "_eventIds": file_info.get("_eventIds"),
            "_entityIds": file_info.get("_entityIds"), 
            "mimeType": file_info.get("mime_type"),
            "path": fileUrl,
            "bucket": AZURE_CONTAINER_NAME,
            "objectName": '/'.join(file_info["file_url"].split("/")[4:]),
            "fileUrl": fileUrl,
            "storageId": file_info.get("id"),
        }

        if 'image' in file_info['mime_type']:
            doc['tags'] = ["auto_analysis_images"]

        if 'video' in file_info["mime_type"]:
            thumbnail_url = generate_thumbnail(file_info["id"], file_info["file_url"])
            doc["thumbnail_url"] = "/" + "/".join(thumbnail_url.split("/")[3:])
            doc['tags'] = ["auto_analysis_videos"]

        try:
            resp = ds_client.docs.insert_document(collection=FILE_COLLECTION, doc=doc)
            return True
        except Exception as e:
            logging.error(f"Error while uploading file_collection schema: {e}")
    
    return False

def generate_thumbnail(id, video_url):
    vid_filename = f"{id}.mp4"
    vid_filepath = os.path.join("temp", vid_filename)

    vid_mime, vid_filename, vid_filepath = download_file(url=video_url, fName=vid_filename, fPath=vid_filepath)
    
    full_filename = f"{id}.jpeg"
    filepath = os.path.join("temp", full_filename)
    content_type = "image/jpeg"

    clip = mp.VideoFileClip(vid_filepath)
    thumbnail = clip.save_frame(filepath, t=0.3)
    clip.reader.close()
    clip.audio.reader.close_proc()

    thumbnail_url = None
    if ON_PREM_UPLOAD:
        if USE_ASYNC_DOWNLOAD:
            try:
                f = ds_client.files.upload_file(file_path=filepath, file_id=id)
                thumbnail_url = FS_BASE_URL + f['fileUrl']
                if not f:
                    raise Exception(f"Failed to upload thumbnail for video: {video_url}")

            except Exception as e:
                logging.error(f"thumbnail Upload failed! {video_url}, Exception - {e}")
    else:
        az_data = upload_to_azure_blob(filename=full_filename, content_type=content_type, local_filepath=filepath)
        thumbnail_url = az_data['fileUrl']

        if thumbnail_url:
            logging.info(f"Uploaded thumbnail for {video_url}")
        else:
            logging.error(f"Thumbnail upload failed to Azure blob storage for URL: {video_url} | Error: {f}")    
    try:
        os.remove(filepath)
    except Exception as et:
        logging.error(f"thumbnail image delete failed for path: {filepath}. Error: {et}")
    
    try:
        os.remove(vid_filepath)
    except Exception as ev:
        logging.error(f"thumbnail-video delete failed for path: {vid_filepath}. Error: {ev}")
    return thumbnail_url