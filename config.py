import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

INGEST_DOCS=True
DUMP_OUTPUT=False
DOCUMENT_COLLECTION = 'Document'
SHORT_URLS_COLLECTION = 'shorturls'
ATTACHMENT_QUEUE = 'attachment_queue'
ENTITY_CARD_COLLECTION = os.getenv("ENTITY_CARD_COLLECTION", "EntityCard")
FILE_COLLECTION = os.getenv("FILE_COLLECTION", "File")
ATTACHMENT_SERVICE_PORT=os.getenv('ATTACHMENT_SERVICE_PORT', 3000)
CYBERSMART_DOCSERVER = os.getenv("DOC_SERVER_BASE_URL","https://cybersmart.s2t.ai/docserver")
ES_URL=os.getenv("ES_URL","https://elasticsearch.s2t.ai")
ES_USER=os.getenv("ES_USER","crawler_user")
ES_PASSWORD=os.getenv("ES_PASSWORD","k2ZsNCLRP5Q=")
MINIO_URL=os.getenv("MINIO_URL","fs.s2t.ai:443")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY","crawler_user")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY","k2ZsNCLRP5Q=")
MINIO_PROXY_MODE= os.getenv("MINIO_PROXY_MODE", "false").strip().lower()=="true"
FS_BASE_URL=os.getenv("FS_BASE_URL","https://cybersmart.s2t.ai/fs")
MINIO_SECURE=os.getenv("MINIO_SECURE","true").strip().lower()=="true"

DB_NAME = os.getenv("DB_NAME", "attachment_service")
URLS_COLLECTION = os.getenv("URLS_COLLECTION", "attachment_urls")
RECRAWL_COLLECTION = os.getenv("RECRAWL_COLLECTION", "recrawl_queue")
RECOVER_COLLECTION = os.getenv("RECOVER_COLLECTION", "recover_queue")
B64_COLLECTION = os.getenv("B64_COLLECTION", "b64_queue")
LISTENER_THREAD_COUNT = int(os.getenv("LISTENER_THREAD_COUNT", 64))
USE_ASYNC_DOWNLOAD = os.getenv("USE_ASYNC_DOWNLOAD", "true").strip().lower()=="true"
ON_PREM_UPLOAD = os.getenv("ON_PREM_UPLOAD", "true").strip().lower()=="true"
LISTENER_SLEEP_TIME = int(os.getenv("LISTENER_SLEEP_TIME", 10))
PROCESSING_CHECK_HOURS = int(os.getenv("PROCESSING_CHECK_HOURS", 1))

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING", f'mongodb://{MONGO_HOST}:{MONGO_PORT}')

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONTAINER_STRING", "2v4lm+uaCs6/iujvE/QaoN9sQ4/07JLXxG0QnptsTyXSpfkjhIXBzsMbBh4rSa00lKswXBD2ciDlN6PLeinwdg==Connection Sting1:DefaultEndpointsProtocol=https;AccountName=databuzzblob01;AccountKey=2v4lm+uaCs6/iujvE/QaoN9sQ4/07JLXxG0QnptsTyXSpfkjhIXBzsMbBh4rSa00lKswXBD2ciDlN6PLeinwdg==;EndpointSuffix=core.windows.net")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "attachments-dev")
AZURE_ACCOUNT_NAME= AZURE_CONNECTION_STRING.split(";")[1].split("=")[1]
ENQUEUE_PROCESSING = os.getenv("ENQUEUE_PROCESSING", "true").strip().lower()=="true"

SORT_ENQUEUED_BY_LATEST = os.getenv("SORT_ENQUEUED_BY_LATEST", "true").strip().lower()=="true"

GENERIC_FILE_RESP_SOURCES = os.getenv('GENERIC_FILE_RESP_SOURCES', "cdn.botdef.com, cdninstagram.com")

REDIS_HOST = os.getenv('REDIS_HOST', '')
REDIS_PORT = int(os.getenv('REDIS_PORT', ''))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', '')
REDIS_DB = int(os.getenv('REDIS_DB', 0))

PROXY_URL = os.getenv("PROXY_URL", "http://brd-customer-hl_3d7ceccc-zone-twint_dc:1gahu9xq5bre@zproxy.lum-superproxy.io:22225")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "6628568379|c1e620fa708a1d5696fb991c1bde5662")
SERVER_BASE_URL = os.getenv("SERVER_BASE_URL", "https://cybersmart.s2t.ai")
ROOT_PREFIX = os.getenv("ROOT_PREFIX", "/")
