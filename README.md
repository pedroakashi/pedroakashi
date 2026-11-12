### Hi, my name is Pedro
(Olá, me chamo Pedro)

Current,i'm studying Computer Science at IMT

(Atualmente, estou estudando Ciencia da Computação no IMT)

[![Gmail](https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white)](https://mail.google.com/mail/u/0/#inbox?compose=new)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/pedro-akashi-241b67254/)

![Pedro's GitHub stats](https://github-readme-stats.vercel.app/api?username=pedroakashi&show_icons=true&theme=dracula)

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

