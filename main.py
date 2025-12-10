#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api.routes import app
from config import Config
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Data Retrieval System")
    logger.info(f"API Host: {Config.API_HOST}")
    logger.info(f"API Port: {Config.API_PORT}")
    logger.info(f"MongoDB URI: {Config.MONGO_URI}")
    logger.info(f"Database: {Config.DATABASE_NAME}")
    # comment here to force commit again.
    try:
        app.run(host=Config.API_HOST, port=Config.API_PORT, debug=False)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
