import asyncio
import base64
import hashlib
import logging
import requests
import api.utils as utils
from config import *


def process_attachments(url_id, url, metadata):
    if ON_PREM_UPLOAD:
        if USE_ASYNC_DOWNLOAD:
            try:
                # Calls AioHttp based async download and upload function
                asyncio.run(utils.uploader_main([{url_id: url}]))
            except Exception as e:
                print(f"Exception in async call to upload: {e}")
        else:
            # Calls generic download and upload function
            utils.upload_files({url_id: url})
    else:
        retries = 3
        fs_url = False
        f = ""
        found_gen = False
        for domain in GENERIC_FILE_RESP_SOURCES.split(','):
            if domain.strip() in url:
                found_gen = True
                break

        content_type, filename, filepath = utils.download_file(url=url, fName=url_id)
        if not found_gen:

            while retries:
                try:
                    # Tries to upload blob with the exact input URL
                    f = utils.upload_to_azure_blob_with_copy(filename=url_id, url=url)
                    if isinstance(f, str):
                        # if input URL is masked and redirects/ isn't accessible from plain input URL
                        # Then get the final redirected URL and retry
                        try:
                            resp = requests.get(url, timeout=120)
                            final_url = resp.url
                        except:
                            raise Exception
                        
                        if final_url != url:
                            f = utils.upload_to_azure_blob_with_copy(filename=url_id, url=final_url)

                    if not isinstance(f, dict):
                        raise Exception
                    fs_url = f['fileUrl']
                    break
                except:
                    retries -= 1

        try:
            if not fs_url:
                # If copy blob from URL doesn't work,
                # use generic download function and upload file to Azure
                # then delete the file
                f = utils.upload_to_azure_blob(filename=filename, content_type=content_type, local_filepath=filepath)
                if not isinstance(f, dict):
                    raise Exception
                fs_url = f['fileUrl']

        except:
            pass

        if filepath != "":
            try:
                os.remove(filepath)
            except Exception as delExp:
                logging.error(f"Delete failed for {filepath}: {delExp}")
                pass

        if fs_url:
            logging.info(f"Uploaded {url}")
            utils.add_fs_url(fs_url=fs_url, url_id=url_id)
            utils.upload_to_file_collection({"id": url_id,
                                            "file_type":  content_type.split("/")[0],
                                            "mime_type": content_type,
                                            "file_url": fs_url,
                                            "_eventIds": metadata.get("_eventIds"), 
                                            "_entityIds": metadata.get("_entityIds")
                                            })
        else:
            logging.error(f"Upload failed to Azure blob storage for URL: {url} | Error: {f}")
            utils.update_status(status='error', url_id=url_id)


def process_b64(url_id, url, b64_str, metadata):
    filename = hashlib.md5(url.encode("utf-8")).hexdigest()
    full_filename = f"{filename}.jpeg"
    filepath = os.path.join("temp", full_filename)
    content_type = "img/jpeg"
    try:
        decoded_string = base64.b64decode(b64_str)

        with open(filepath, "wb") as f:
            f.write(decoded_string)
    except Exception as te:
        logging.error(f"Error while creating file from b64 string: {te}")
    
    if ON_PREM_UPLOAD:
        if USE_ASYNC_DOWNLOAD:
            try:
                f = utils.ds_client.files.upload_file(file_path=filepath, file_id=filename)
                fs_url = FS_BASE_URL + f['fileUrl']
                if not f:
                    raise Exception(f"Failed to upload: {url}")

            except Exception as e:
                logging.error(f"[b64] Upload failed! {url}, Exception - {e}")
    else:

        az_data = utils.upload_to_azure_blob(filename=full_filename, content_type=content_type, local_filepath=filepath)
        fs_url = az_data['fileUrl']

        if fs_url:
            logging.info(f"Uploaded {url}")
            utils.add_fs_url(fs_url=fs_url, url_id=url_id)

            utils.upload_to_file_collection({"id": url_id,
                                            "file_type":  content_type.split("/")[0],
                                            "mime_type": content_type,
                                            "file_url": fs_url,
                                            "_eventIds": metadata.get("_eventIds"), 
                                            "_entityIds": metadata.get("_entityIds")
                                            })
        else:
            logging.error(f"Upload failed to Azure blob storage for URL: {url} | Error: {f}")
            utils.update_status(status='error', url_id=url_id)
    
    try:
        os.remove(filepath)
    except:
        pass


def process_recrawl_requests(url_id, url, metadata):
    if ON_PREM_UPLOAD:
        if USE_ASYNC_DOWNLOAD:
            try:
                # Calls AioHttp based async download and upload function
                asyncio.run(utils.uploader_main([{url_id: url}]))
            except Exception as e:
                print(f"Exception in async call to upload: {e}")
        else:
            # Calls generic download and upload function
            utils.upload_files({url_id: url})
    else:
        retries = 3
        fs_url = False
        f = ""
        found_gen = False
        for domain in GENERIC_FILE_RESP_SOURCES.split(','):
            if domain.strip() in url:
                found_gen = True
                break

        content_type, filename, filepath = utils.download_file(url=url, fName=url_id)
        if not found_gen:
            while retries:
                try:
                    # Tries to upload blob with the exact input URL
                    f = utils.upload_to_azure_blob_with_copy(filename=url_id, url=url)
                    if isinstance(f, str):
                        # if input URL is masked and redirects/ isn't accessible from plain input URL
                        # Then get the final redirected URL and retry
                        try:
                            proxies = {"http": PROXY_URL, "https": PROXY_URL}
                            resp = requests.get(url, timeout=120, proxies=proxies)
                            final_url = resp.url
                        except Exception as e:
                            logging.error(e)
                            raise Exception
                        f = utils.upload_to_azure_blob_with_copy(filename=url_id, url=final_url)

                    if not isinstance(f, dict):
                        raise Exception
                    fs_url = f['fileUrl']
                    break
                except:
                    retries -= 1

        try:
            if not fs_url:
                # If copy blob from URL doesn't work,
                # use generic download function and upload file to Azure
                # then delete the file
                
                f = utils.upload_to_azure_blob(filename=filename, content_type=content_type, local_filepath=filepath)
                if not isinstance(f, dict):
                    raise Exception
                fs_url = f['fileUrl']

        except:
            pass

        if filepath != "":
            try:
                os.remove(filepath)
            except Exception as delExp:
                logging.error(f"Delete failed for {filepath}: {delExp}")
                pass
            
        # status = utils.get_status(url_id)

        if fs_url:
            logging.info(f"Uploaded {url}")
            utils.add_fs_url(fs_url=fs_url, url_id=url_id)

            utils.upload_to_file_collection({"id": url_id,
                                            "file_type":  content_type.split("/")[0],
                                            "mime_type": content_type,
                                            "file_url": fs_url,
                                            "_eventIds": metadata.get("_eventIds"), 
                                            "_entityIds": metadata.get("_entityIds")
                                            })
            
            if ("EntityCard" in metadata) and ("updateField" in metadata):
                if ROOT_PREFIX == "/":
                    mapped_url = f"{SERVER_BASE_URL}/dl/{url_id}"
                else:
                    mapped_url = f"{SERVER_BASE_URL}{ROOT_PREFIX}/dl/{url_id}"
                resp = utils.update_ec_url(metadata['EntityCard'], doc={metadata['updateField']: mapped_url})
                if isinstance(resp, dict):
                    logging.info(resp)
                else:
                    logging.error(f"Error while updating fs_url in entityCard: {resp}")
        else:
            logging.error(f"Upload failed to Azure blob storage for URL: {url} | Error: {f}")
            utils.update_status(status='error', url_id=url_id)