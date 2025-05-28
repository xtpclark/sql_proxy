
import asyncio
import logging
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from .server import ProxyServer
from .config import CONFIG

os.makedirs(os.path.dirname(CONFIG.proxy_config["log_file"]), exist_ok=True)
logging.basicConfig(
    level=getattr(logging, CONFIG.proxy_config["log_level"], logging.DEBUG),
    format='[%(asctime)s] [%(levelname)s] [%(name)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S%z',
    handlers=[
        logging.FileHandler(CONFIG.proxy_config["log_file"]),
        logging.StreamHandler()
    ]
)

async def main():
    proxy = ProxyServer()
    await proxy.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt caught, shutting down")
    except Exception as e:
        logging.critical(f"Critical error: {e}", exc_info=True)
