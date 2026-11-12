"""Roda a Lambda localmente com o evento de teste, SEM chamar a AWS nem o IM8 (tudo simulado).

Uso (dentro da pasta app):  python run_local.py
"""
import json
import os
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("LOG_JSON", "false")
os.environ.setdefault("URL_IM8_CURVAS", "https://curvas-v1-distribuiprecoscorporativos.api-sp.dev.aws.cloud.ihf")

import lambda_function  # noqa: E402


def resposta_falsa(self, url, params=None, **kwargs):
    print(f"\n>>> GET {url}  params={params}")
    print(f"    headers: {sorted(kwargs['headers'])}")
    return SimpleNamespace(status_code=200, reason="OK", url=url,
                           json=lambda: RESPOSTA_IM8_SIMULADA)


# Formato SIMULADO: trocar por uma resposta real da API de curvas assim que houver.
RESPOSTA_IM8_SIMULADA = {"pontos": [
    {"prazo_dias": 1,   "taxa": "10.6500", "indicador_interpolado": ""},
    {"prazo_dias": 30,  "taxa": "10.7200", "indicador_interpolado": ""},
    {"prazo_dias": 45,  "taxa": "10.7450", "indicador_interpolado": "S"},
    {"prazo_dias": 90,  "taxa": "10.8100", "indicador_interpolado": ""},
    {"prazo_dias": 135, "taxa": "10.8600", "indicador_interpolado": "S"},
    {"prazo_dias": 180, "taxa": "10.9300", "indicador_interpolado": ""},
    {"prazo_dias": 360, "taxa": "11.1500", "indicador_interpolado": ""},
]}


with open("evento_teste.json", encoding="utf-8") as f:
    evento = json.load(f)

with patch.object(lambda_function, "obtain_client_credentials",
                  return_value=SimpleNamespace(client_id="client-teste", client_secret="x")), \
     patch.object(lambda_function.StsService, "obtain_sts_token", return_value="token-teste"), \
     patch("requests.Session.get", resposta_falsa):
    resultado = lambda_function.lambda_handler(evento, None)

print("\nRESULTADO:", json.dumps(resultado, indent=2, ensure_ascii=False))

# Guarda as mensagens que iriam para a SQS, para a Lambda de gravação usar no teste local dela.
with open("mensagens_sqs.json", "w", encoding="utf-8") as f:
    json.dump([r["mensagem_sqs"] for r in resultado if "mensagem_sqs" in r], f, indent=2, ensure_ascii=False)
print("\nMensagens SQS salvas em mensagens_sqs.json")
