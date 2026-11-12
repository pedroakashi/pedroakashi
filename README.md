import sys
import os
import time
from loguru import logger
from sys import platform

if platform == "linux" or platform == "linux2":
    os.environ['TZ'] = 'America/Sao_Paulo'
    time.tzset()

logger.remove()
format_template = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level>\
 | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

# Na AWS sai em JSON (serialize=True, igual à obterdadospreco). No teste local, LOG_JSON=false deixa legível.
logger.add(sink=sys.stderr, backtrace=False, diagnose=False, format=format_template,
           serialize=os.getenv("LOG_JSON", "true").lower() == "true")
           
