# Teste da captura de curvas na AWS (`teste_aws_console`)

Lambda de captura de curvas (`obterdadoscurvas`) para testar em DEV a chamada à API de curvas do IM8.
Usa só Python padrão + boto3 (sem bibliotecas externas). Todos os arquivos ficam na mesma pasta, sem subpastas.

Fluxo: evento do Connector → valida a curva → credenciais (`InfraSecretsMX3`) → token (STS) → API de curvas do IM8 → filtra os pilares.
Somente leitura: não envia para fila e não grava nada.

## `lambda_function.py`

Handler: junta as etapas e devolve o resultado com diagnóstico da resposta.

```python
"""
obterdadoscurvas — versão para o console da AWS (só Python padrão + boto3).

Fluxo: evento do Connector -> valida a curva -> credenciais (Secrets Manager) -> token (STS)
       -> API de curvas do IM8 -> filtra os pilares.
Somente leitura: NÃO envia para fila e NÃO grava nada.
"""
import uuid

from credentials_service import obter_credenciais
from curva_event_model import EventoInvalido, ler_curva
from im8_curvas_service import obter_pontos
from logger_config import log
from pilares_service import filtrar_pilares
from sts_service import obter_token


def lambda_handler(event, context):
    correlation_id = str(uuid.uuid1())
    registros = event if isinstance(event, list) else [event]
    log("INFO", "Início da lambda ObterDadosCurvas (teste console).", correlation_id=correlation_id,
        total_eventos=len(registros))

    client_id, client_secret = obter_credenciais()
    token = obter_token(client_id, client_secret)

    resultados = []
    for elemento in registros:
        try:
            curva = ler_curva(elemento["payload"]["value"])
        except (EventoInvalido, KeyError, TypeError) as e:
            log("ERROR", "Evento de curva inválido, descartado.", erro=str(e))
            resultados.append({"status": "INVALIDO", "erro": str(e)})
            continue
        try:
            resposta, url = obter_pontos(curva, token, client_id, correlation_id)
        except RuntimeError as e:
            log("ERROR", "Falha ao obter pontos.", codigo_curva=curva["codigo_curva"], erro=str(e))
            resultados.append({"codigo_curva": curva["codigo_curva"], "status": "ERRO", "erro": str(e)})
            continue

        pontos, pilares, campo = filtrar_pilares(resposta)
        primeiro = pontos[0] if pontos and isinstance(pontos[0], dict) else {}
        log("INFO", "Curva capturada.", codigo_curva=curva["codigo_curva"], total_pontos=len(pontos),
            total_pilares=len(pilares))
        resultados.append({
            "codigo_curva": curva["codigo_curva"], "nome_curva": curva["nome_curva"], "status": "CAPTURADA",
            "url_chamada": url, "total_pontos": len(pontos), "total_pilares": len(pilares),
            # diagnóstico para confirmar o formato real da resposta:
            "chaves_da_resposta": list(resposta.keys()) if isinstance(resposta, dict) else "lista",
            "campos_de_cada_ponto": list(primeiro.keys()),
            "campo_interpolado_existe": campo in primeiro,
            "amostra_pontos": pontos[:5],
        })

    log("INFO", "Término.", correlation_id=correlation_id)
    return {"correlation_id": correlation_id, "resultados": resultados}
```

## `logger_config.py`

Log em JSON (sem biblioteca externa).

```python
import json


def log(nivel, mensagem, **campos):
    """Log em JSON no CloudWatch (sem biblioteca externa)."""
    print(json.dumps({"level": nivel, "message": mensagem, **campos}, ensure_ascii=False, default=str))
```

## `curva_event_model.py`

Validação do evento de curva.

```python
import re
from datetime import datetime

TIPOS_VALIDOS = ("BASE", "COMPOSTA", "TRANSFER")


class EventoInvalido(Exception):
    pass


def _para_yyyymmdd(texto):
    if re.fullmatch(r"\d{8}", texto):
        return texto
    if re.match(r"\d{4}-\d{2}-\d{2}", texto):
        return datetime.strptime(texto[:10], "%Y-%m-%d").strftime("%Y%m%d")
    raise EventoInvalido(f"data_liberacao inválida: {texto}")


def ler_curva(value):
    """Valida o evento de curva (payload.value do Connector) e devolve os dados normalizados."""
    for campo in ("codigo_curva", "nome_curva", "nome_tipo_curva", "data_liberacao", "data_inclusao"):
        if not str(value.get(campo) or "").strip():
            raise EventoInvalido(f"campo obrigatório ausente: {campo}")
    tipo = str(value["nome_tipo_curva"]).strip().upper()
    if tipo not in TIPOS_VALIDOS:
        raise EventoInvalido(f"nome_tipo_curva desconhecido: {tipo}")
    return {"codigo_curva": str(value["codigo_curva"]).strip(), "nome_curva": str(value["nome_curva"]).strip(),
            "tipo_curva": tipo, "data_liberacao": _para_yyyymmdd(str(value["data_liberacao"]).strip()),
            "data_inclusao": str(value["data_inclusao"]).strip()}
```

## `http_client.py`

Chamada HTTP com o certificado do banco.

```python
import os
import ssl
import time
import urllib.error
import urllib.request


def http(metodo, url, headers=None, corpo=None, timeout=15):
    """Faz a requisição com o certificado do banco (CA_CERT_PATH). Devolve (status, texto, tempo_ms).
    Erro de rede/SSL/timeout volta com status None."""
    cafile = os.getenv("CA_CERT_PATH")
    contexto = ssl.create_default_context(cafile=cafile) if cafile and os.path.exists(cafile) else ssl.create_default_context()
    req = urllib.request.Request(url, data=corpo.encode("utf-8") if corpo else None, headers=headers or {}, method=metodo)
    inicio = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=contexto) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace"), int((time.time() - inicio) * 1000)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace"), int((time.time() - inicio) * 1000)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}", int((time.time() - inicio) * 1000)
```

## `credentials_service.py`

Credenciais no Secrets Manager (InfraSecretsMX3).

```python
import json
import os

import boto3


def obter_credenciais():
    """Lê client_id e client_secret no segredo compartilhado (SECRET_MANAGER_NAME), uma chave por lambda."""
    cliente = boto3.client("secretsmanager", region_name=os.getenv("AWS_REGION_NAME", "sa-east-1"))
    segredo = json.loads(cliente.get_secret_value(SecretId=os.getenv("SECRET_MANAGER_NAME"))["SecretString"])
    chave_id, chave_secret = os.getenv("AWS_LAMBDA_CLIENT_ID"), os.getenv("AWS_LAMBDA_SECRET_ID")
    if chave_id not in segredo or chave_secret not in segredo:
        raise RuntimeError(f"chaves {chave_id} / {chave_secret} não existem no segredo {os.getenv('SECRET_MANAGER_NAME')}")
    return segredo[chave_id], segredo[chave_secret]
```

## `sts_service.py`

Token no STS.

```python
import json
import os
import urllib.parse

from http_client import http
from logger_config import log


def obter_token(client_id, client_secret):
    """Token no STS, mesmo protocolo da obterdadospreco: GET com corpo form-urlencoded."""
    url = os.getenv("URLSts").rstrip("/") + "/api/oauth/token"
    corpo = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": client_id,
                                    "client_secret": client_secret})
    status, texto, ms = http("GET", url, {"Content-Type": "application/x-www-form-urlencoded"}, corpo, timeout=10)
    log("INFO", "Resposta do STS.", status=status, tempo_ms=ms)
    if status != 200:
        raise RuntimeError(f"STS falhou: status={status} detalhe={texto[:200]}")
    return json.loads(texto)["access_token"]
```

## `im8_curvas_service.py`

Chamada à API de curvas do IM8.

```python
import json
import os
import urllib.parse

from http_client import http
from logger_config import log


def obter_pontos(curva, token, client_id, correlation_id):
    """TRANSFER -> pontotransferencias (pelo código); BASE/COMPOSTA -> pontoscurvas (pelo nome)."""
    base = os.getenv("URL_IM8_CURVAS").rstrip("/") + "/curvas/v1"
    if curva["tipo_curva"] == "TRANSFER":
        url = f"{base}/pontotransferencias?" + urllib.parse.urlencode(
            {"codigocurva": curva["codigo_curva"], "dataexecucaocurva": curva["data_liberacao"]})
    else:
        url = f"{base}/pontoscurvas/{urllib.parse.quote(curva['nome_curva'], safe='')}?" + urllib.parse.urlencode(
            {"data_execucao": curva["data_liberacao"]})

    headers = {"Authorization": "Bearer " + token, "x-itau-apikey": client_id,
               "x-itau-correlationID": correlation_id, "Accept": "application/json"}
    if os.getenv("X_APIGW_API_ID"):
        headers["x-apigw-api-id"] = os.getenv("X_APIGW_API_ID")

    status, texto, ms = http("GET", url, headers)
    log("INFO", "Resposta da API de curvas do IM8.", codigo_curva=curva["codigo_curva"], url=url,
        status=status, tempo_ms=ms, tamanho_bytes=len(texto))
    if status != 200:
        raise RuntimeError(f"API de curvas falhou: status={status} url={url} detalhe={texto[:300]}")
    return json.loads(texto), url
```

## `pilares_service.py`

Filtro de pilares (pontos não interpolados).

```python
import os


def _lista_de_pontos(resposta):
    if isinstance(resposta, list):
        return resposta
    if isinstance(resposta, dict):
        if isinstance(resposta.get("pontos"), list):
            return resposta["pontos"]
        for valor in resposta.values():          # formato real ainda desconhecido: pega a 1ª lista
            if isinstance(valor, list):
                return valor
    return []


def filtrar_pilares(resposta):
    """Mantém só os pontos que o IM8 NÃO marcou como interpolados (pilares da curva)."""
    campo = os.getenv("CAMPO_INTERPOLADO", "indicador_interpolado")
    pontos = _lista_de_pontos(resposta)
    pilares = [p for p in pontos if isinstance(p, dict)
               and str(p.get(campo) or "").strip().upper() in ("", "N", "FALSE", "0")]
    return pontos, pilares, campo
```

## `evento_teste_console.json`

Evento de teste no formato do Connector (curva 2361 / BRL_KLm0_RM01). Colar na aba Testar.

```json
[
  {
    "payload": {
      "key": null,
      "value": {
        "codigo_curva": "2361",
        "nome_curva": "BRL_KLm0_RM01",
        "nome_tipo_curva": "BASE",
        "data_liberacao": "20261112",
        "data_inclusao": "2026-11-12T18:05:00Z",
        "data_execucao_processo": "2026-11-12T18:00:00Z"
      }
    }
  }
]
```
