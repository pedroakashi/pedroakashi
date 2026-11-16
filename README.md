# Captura de Curvas IM8 (`testeaws`)

Lambda de captura de curvas do IM8 para o Murex, seguindo o *Guia de Implementação v1.0* e o padrão da `obterdadospreco`.
Usa só Python padrão + boto3 (sem bibliotecas externas). Todos os arquivos ficam na mesma pasta, sem subpastas.

**Fluxo:** evento do Connector → valida a curva → credenciais (`InfraSecretsMX3`) → token (STS) → tipo pelo cadastro (se faltar) → pontos na API de curvas → filtra os pilares.

**Status:** código pronto; falta o clientID das curvas para a chamada real. A rede de dev já alcança o STS e a API de curvas.

## Variáveis de ambiente

| Chave | Valor |
|---|---|
| `SECRET_MANAGER_NAME` | `InfraSecretsMX3` |
| `AWS_LAMBDA_CLIENT_ID` | nome da chave do clientID das curvas no secret (pendente) |
| `AWS_LAMBDA_SECRET_ID` | nome da chave do secret das curvas no secret (pendente) |
| `URL_IM8_CURVAS` | `https://curvas-v1-distribuiprecoscorporativos.api-sp.dev.aws.cloud.ihf` |
| `URLSts` | `https://des-sts-int.mbi.cloud.ihf` |
| `CA_CERT_PATH` | `/opt/certs/ca_bundle.crt` |
| `AWS_REGION_NAME` | `sa-east-1` |
| `X_APIGW_API_ID` | opcional (não consta no guia) |
| `CAMPO_INTERPOLADO` | opcional; padrão `indicador_interpolado` (confirmar com resposta real) |

## Arquivos

### `lambda_function.py`

Handler (`lambda_function.lambda_handler`): junta as etapas. Se a curva vier sem tipo, consulta o cadastro antes de buscar os pontos.

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
from im8_curvas_service import obter_pontos, obter_tipo_cadastro
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
            if not curva["tipo_curva"]:
                curva["tipo_curva"] = obter_tipo_cadastro(curva, token, client_id, correlation_id)
                log("INFO", "Tipo resolvido pelo cadastro.", codigo_curva=curva["codigo_curva"],
                    tipo_curva=curva["tipo_curva"])
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
            "codigo_curva": curva["codigo_curva"], "nome_curva": curva["nome_curva"],
            "tipo_curva": curva["tipo_curva"], "status": "CAPTURADA",
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

### `logger_config.py`

Log em JSON no CloudWatch, sem biblioteca externa.

```python
import json


def log(nivel, mensagem, **campos):
    """Log em JSON no CloudWatch (sem biblioteca externa)."""
    print(json.dumps({"level": nivel, "message": mensagem, **campos}, ensure_ascii=False, default=str))
```

### `curva_event_model.py`

Valida o evento de curva (`payload.value` do Connector). `nome_tipo_curva` é opcional.

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
    """Valida o evento de curva (payload.value do Connector) e devolve os dados normalizados.
    nome_tipo_curva é opcional: se faltar, o tipo é resolvido pelo cadastrocurvas (guia, 6.4)."""
    for campo in ("codigo_curva", "nome_curva", "data_liberacao", "data_inclusao"):
        if not str(value.get(campo) or "").strip():
            raise EventoInvalido(f"campo obrigatório ausente: {campo}")
    tipo = str(value.get("nome_tipo_curva") or "").strip().upper() or None
    if tipo and tipo not in TIPOS_VALIDOS:
        raise EventoInvalido(f"nome_tipo_curva desconhecido: {tipo}")
    return {"codigo_curva": str(value["codigo_curva"]).strip(), "nome_curva": str(value["nome_curva"]).strip(),
            "tipo_curva": tipo, "data_liberacao": _para_yyyymmdd(str(value["data_liberacao"]).strip()),
            "data_inclusao": str(value["data_inclusao"]).strip()}
```

### `http_client.py`

Chamada HTTP com o certificado do banco (`CA_CERT_PATH`) e descompressão gzip.

```python
import gzip
import os
import ssl
import time
import urllib.error
import urllib.request
import zlib


def _descomprimir(dados, encoding):
    """O guia (6.2) pede que o cliente suporte descompressão da resposta."""
    encoding = (encoding or "").lower()
    if encoding == "gzip":
        return gzip.decompress(dados)
    if encoding == "deflate":
        return zlib.decompress(dados)
    return dados


def http(metodo, url, headers=None, corpo=None, timeout=15):
    """Faz a requisição com o certificado do banco (CA_CERT_PATH). Devolve (status, texto, tempo_ms).
    Erro de rede/SSL/timeout volta com status None."""
    cafile = os.getenv("CA_CERT_PATH")
    contexto = ssl.create_default_context(cafile=cafile) if cafile and os.path.exists(cafile) else ssl.create_default_context()
    req = urllib.request.Request(url, data=corpo.encode("utf-8") if corpo else None, headers=headers or {}, method=metodo)
    inicio = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=contexto) as resp:
            dados = _descomprimir(resp.read(), resp.headers.get("Content-Encoding"))
            return resp.status, dados.decode("utf-8", errors="replace"), int((time.time() - inicio) * 1000)
    except urllib.error.HTTPError as e:
        dados = _descomprimir(e.read(), e.headers.get("Content-Encoding") if e.headers else None)
        return e.code, dados.decode("utf-8", errors="replace"), int((time.time() - inicio) * 1000)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}", int((time.time() - inicio) * 1000)
```

### `credentials_service.py`

Lê client_id e client_secret no Secrets Manager (`InfraSecretsMX3`).

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

### `sts_service.py`

Obtém o token no STS, mesmo protocolo da obterdadospreco.

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

### `im8_curvas_service.py`

Chama a API de curvas do IM8: `cadastrocurvas`, `pontoscurvas` (BASE/COMPOSTA) e `pontotransferencias` (TRANSFER). Cabeçalhos da seção 6.2 do guia.

```python
import json
import os
import urllib.parse

from curva_event_model import TIPOS_VALIDOS
from http_client import http
from logger_config import log

# Nome do campo de tipo na resposta do cadastrocurvas: PROVISÓRIO até ver uma resposta real.
CAMPOS_TIPO_CADASTRO = ("nome_tipo_curva", "NomTipoCurv", "nomeTipoCurva", "tipo_curva", "TipCurv")


def _headers(token, client_id, correlation_id):
    """Cabeçalhos da seção 6.2 do guia. x-apigw-api-id é opcional (não consta no guia)."""
    headers = {"Authorization": "Bearer " + token, "x-itau-apikey": client_id,
               "x-itau-correlationID": correlation_id, "Accept": "application/json",
               "Accept-Encoding": "gzip"}
    if os.getenv("X_APIGW_API_ID"):
        headers["x-apigw-api-id"] = os.getenv("X_APIGW_API_ID")
    return headers


def _base():
    return os.getenv("URL_IM8_CURVAS").rstrip("/") + "/curvas/v1"


def _get(url, token, client_id, correlation_id, codigo_curva, nome_chamada):
    status, texto, ms = http("GET", url, _headers(token, client_id, correlation_id))
    log("INFO", f"Resposta da API {nome_chamada} do IM8.", codigo_curva=codigo_curva, url=url,
        status=status, tempo_ms=ms, tamanho_bytes=len(texto))
    if status != 200:
        raise RuntimeError(f"API {nome_chamada} falhou: status={status} url={url} detalhe={texto[:300]}")
    return json.loads(texto)


def obter_tipo_cadastro(curva, token, client_id, correlation_id):
    """Guia 6.4: se o tipo não vier no evento, consulta cadastrocurvas (última versão) e procura o tipo."""
    url = f"{_base()}/cadastrocurvas?" + urllib.parse.urlencode(
        {"NomCurv": curva["nome_curva"], "ApenasUltimaVersao": "true"})
    cadastro = _get(url, token, client_id, correlation_id, curva["codigo_curva"], "cadastrocurvas")

    pendentes = [cadastro]
    while pendentes:
        item = pendentes.pop(0)
        if isinstance(item, dict):
            for campo in CAMPOS_TIPO_CADASTRO:
                valor = str(item.get(campo) or "").strip().upper()
                if valor in TIPOS_VALIDOS:
                    return valor
            pendentes.extend(item.values())
        elif isinstance(item, list):
            pendentes.extend(item)
    raise RuntimeError("tipo da curva não encontrado no cadastrocurvas (conferir o nome do campo na resposta)")


def obter_pontos(curva, token, client_id, correlation_id):
    """TRANSFER -> pontotransferencias (pelo código); BASE/COMPOSTA -> pontoscurvas (pelo nome)."""
    if curva["tipo_curva"] == "TRANSFER":
        url = f"{_base()}/pontotransferencias?" + urllib.parse.urlencode(
            {"codigocurva": curva["codigo_curva"], "dataexecucaocurva": curva["data_liberacao"]})
    else:
        url = f"{_base()}/pontoscurvas/{urllib.parse.quote(curva['nome_curva'], safe='')}?" + urllib.parse.urlencode(
            {"data_execucao": curva["data_liberacao"]})
    return _get(url, token, client_id, correlation_id, curva["codigo_curva"], "pontos"), url
```

### `pilares_service.py`

Filtra os pontos interpolados e mantém só os pilares.

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

### `teste_local.py`

Teste local sem AWS: `python teste_local.py` (simulado) e `python teste_local.py rede` (verifica se a máquina alcança o STS e a API).

```python
"""
Teste local da captura de curvas (pasta teste_aws_console), sem AWS e sem instalar nada.

  python teste_local.py            -> SIMULADO: roda o código inteiro com STS e IM8 de mentira
  python teste_local.py rede       -> REDE: só verifica se sua máquina alcança o STS e a API de curvas
  python teste_local.py rede URL   -> REDE testando também a URL da API de curvas que o El passar

Nenhum modo usa credencial nem grava nada.
"""
import json
import os
import socket
import ssl
import sys
import types
import urllib.error
import urllib.request
from unittest.mock import patch

PASTA = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PASTA)

URL_STS = "https://des-sts-int.mbi.cloud.ihf"
URL_CURVAS_PADRAO = "https://curvas-v1-distribuiprecoscorporativos.api-sp.dev.aws.cloud.ihf"


# --------------------------------------------------------------------------- modo SIMULADO
def modo_simulado():
    # boto3 não está instalado na máquina: cria um substituto só para o import funcionar.
    if "boto3" not in sys.modules:
        try:
            import boto3  # noqa: F401
        except ImportError:
            sys.modules["boto3"] = types.ModuleType("boto3")

    os.environ.update(SECRET_MANAGER_NAME="InfraSecretsMX3", AWS_LAMBDA_CLIENT_ID="CHAVE_CLIENT_ID",
                      AWS_LAMBDA_SECRET_ID="CHAVE_SECRET", URLSts=URL_STS, URL_IM8_CURVAS=URL_CURVAS_PADRAO)
    import credentials_service
    import im8_curvas_service
    import lambda_function
    import sts_service

    def http_falso(metodo, url, headers=None, corpo=None, timeout=15):
        if "oauth" in url:
            return 200, '{"access_token": "token-de-teste"}', 5
        print(f"   -> chamaria: GET {url}")
        resposta = {"pontos": [
            {"prazo_dias": 1, "taxa": "10.65", "indicador_interpolado": ""},
            {"prazo_dias": 45, "taxa": "10.74", "indicador_interpolado": "S"},
            {"prazo_dias": 90, "taxa": "10.81", "indicador_interpolado": ""},
            {"prazo_dias": 135, "taxa": "10.86", "indicador_interpolado": "S"},
            {"prazo_dias": 180, "taxa": "10.93", "indicador_interpolado": ""},
        ]}
        return 200, json.dumps(resposta), 12

    evento_arq = next((f for f in ("evento_teste_3_curvas.json", "evento_teste_console.json")
                       if os.path.exists(os.path.join(PASTA, f))), None)
    if not evento_arq:
        print("Coloque evento_teste_3_curvas.json ou evento_teste_console.json nesta pasta.")
        return
    with open(os.path.join(PASTA, evento_arq), encoding="utf-8") as f:
        evento = json.load(f)

    print(f"\n=== SIMULADO (evento: {evento_arq}) - STS e IM8 de mentira ===\n")
    with patch.object(credentials_service, "obter_credenciais", return_value=("client-teste", "secret-teste")), \
         patch.object(lambda_function, "obter_credenciais", return_value=("client-teste", "secret-teste")), \
         patch.object(sts_service, "http", http_falso), patch.object(im8_curvas_service, "http", http_falso):
        resultado = lambda_function.lambda_handler(evento, None)

    print("\n=== RESULTADO (o que a Lambda devolveria) ===")
    print(json.dumps(resultado, indent=2, ensure_ascii=False))


# --------------------------------------------------------------------------- modo REDE
def testar_endereco(nome, url):
    print(f"\n{nome}: {url}")
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as r:
            print(f"   ALCANÇOU. Respondeu HTTP {r.status}.")
    except urllib.error.HTTPError as e:
        print(f"   ALCANÇOU. Respondeu HTTP {e.code} (normal sem login: a rede chega lá).")
    except urllib.error.URLError as e:
        motivo = e.reason
        if isinstance(motivo, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in str(motivo):
            print("   ALCANÇOU, mas o Python não confia no certificado do banco.")
            print("   (A rede chega. Para a chamada real, vai precisar apontar o certificado do banco.)")
        elif isinstance(motivo, socket.gaierror):
            print("   NÃO ALCANÇOU: o nome não foi encontrado (DNS). Sua máquina não enxerga esse endereço.")
        elif isinstance(motivo, (socket.timeout, TimeoutError)) or "timed out" in str(motivo):
            print("   NÃO ALCANÇOU: tempo esgotado. Provavelmente bloqueio de rede/firewall.")
        else:
            print(f"   NÃO ALCANÇOU: {type(motivo).__name__}: {motivo}")
    except (socket.timeout, TimeoutError):
        print("   NÃO ALCANÇOU: tempo esgotado. Provavelmente bloqueio de rede/firewall.")
    except Exception as e:
        print(f"   ERRO: {type(e).__name__}: {e}")


def modo_rede(url_curvas):
    print("\n=== TESTE DE REDE (sem login, sem credencial) ===")
    testar_endereco("STS", URL_STS + "/api/oauth/token")
    testar_endereco("API de curvas", url_curvas.rstrip("/") + "/curvas/v1/cadastrocurvas")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() == "rede":
        modo_rede(sys.argv[2] if len(sys.argv) > 2 else URL_CURVAS_PADRAO)
    else:
        modo_simulado()
```

### `evento_teste_console.json`

Evento de teste no formato do Connector (curva 2361 / BRL_KLm0_RM01).

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

### `evento_teste_3_curvas.json`

Evento de teste com 3 curvas do catálogo (BRL, EUR e USD).

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
        "data_inclusao": "2026-11-12T18:05:00Z"
      }
    }
  },
  {
    "payload": {
      "key": null,
      "value": {
        "codigo_curva": "2348",
        "nome_curva": "EUR_KLm0_RM01",
        "nome_tipo_curva": "BASE",
        "data_liberacao": "20261112",
        "data_inclusao": "2026-11-12T18:05:00Z"
      }
    }
  },
  {
    "payload": {
      "key": null,
      "value": {
        "codigo_curva": "2305",
        "nome_curva": "USD_KLm0_RM01",
        "nome_tipo_curva": "BASE",
        "data_liberacao": "20261112",
        "data_inclusao": "2026-11-12T18:05:00Z"
      }
    }
  }
]
```
