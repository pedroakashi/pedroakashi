### Hi, my name is Pedro
(Olá, me chamo Pedro)

Current,i'm studying Computer Science at IMT

(Atualmente, estou estudando Ciencia da Computação no IMT)

[![Gmail](https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white)](https://mail.google.com/mail/u/0/#inbox?compose=new)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/pedro-akashi-241b67254/)

![Pedro's GitHub stats](https://github-readme-stats.vercel.app/api?username=pedroakashi&show_icons=true&theme=dracula)

"""Gera o XML das curvas a partir das mensagens salvas pela captura (../obterdadoscurvas/mensagens_sqs.json)."""
import json
import os
import sys

os.environ.setdefault("LOG_JSON", "false")
import lambda_function  # noqa: E402

origem = sys.argv[1] if len(sys.argv) > 1 else "../obterdadoscurvas/mensagens_sqs.json"
with open(origem, encoding="utf-8") as f:
    mensagens = json.load(f)

evento = {"Records": [{"messageId": str(i), "body": json.dumps(m)} for i, m in enumerate(mensagens)]}
for item in lambda_function.lambda_handler(evento, None):
    caminho = f"{item['nome_curva']}_{item['data']}.xml"
    with open(caminho, "w", encoding="utf-8") as f:
        f.write(item["xml"])
    print(f"\n===== {caminho} =====\n{item['xml']}")



