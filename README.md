### Hi, my name is Pedro
(Olá, me chamo Pedro)

Current,i'm studying Computer Science at IMT

(Atualmente, estou estudando Ciencia da Computação no IMT)

[![Gmail](https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white)](https://mail.google.com/mail/u/0/#inbox?compose=new)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/pedro-akashi-241b67254/)

![Pedro's GitHub stats](https://github-readme-stats.vercel.app/api?username=pedroakashi&show_icons=true&theme=dracula)

import json
import os
import xml.dom.minidom
from logger_config import logger
from xml_curva_service import gerar_xml


def obter_template():
    """No ambiente AWS virá do bucket de templates (como TEMPLATES_MUREX_S3_KEY na enviardadospreco)."""
    with open(os.getenv("TEMPLATE_CURVA_LOCAL", "curva_template.xml"), encoding="utf-8") as f:
        return f.read()


def lambda_handler(event, context):
    template = obter_template()
    xmls = []
    for record in event.get("Records", []):
        mensagem = json.loads(record["body"])
        curva, pilares = mensagem["curva"], mensagem["pilares"]

        conteudo = gerar_xml(template, curva, pilares)
        xml.dom.minidom.parseString(conteudo.encode("utf-8"))   # garante XML bem-formado
        logger.info("XML da curva gerado.", codigo_curva=curva["codigo_curva"], total_pilares=len(pilares))
        xmls.append({"nome_curva": curva["nome_curva"], "data": curva["data_liberacao"], "xml": conteudo})
        # Destino (API on-premises do Murex ou EFS) ainda em definição.
    return xmls

