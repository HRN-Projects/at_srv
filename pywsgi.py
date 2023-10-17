import main
from config import *

app = main.app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", reload=False, host='0.0.0.0', port=ATTACHMENT_SERVICE_PORT, log_config='log.ini')