#!/usr/bin/env python3
"""
Flask Application Entry Point
Starts the Data Retrieval System API server.
"""

from api.routes import app
from config import Config
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info(f"Starting Flask API server on {Config.API_HOST}:{Config.API_PORT}")
    logger.info(f"API Documentation available at: http://{Config.API_HOST}:{Config.API_PORT}/docs")

    app.run(
        host=Config.API_HOST,
        port=Config.API_PORT,
        debug=True
    )
