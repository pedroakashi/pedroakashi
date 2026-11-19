# Captura de Curvas IM8 (`testeaws` / obterdadoscurvas)

Lambda de captura de curvas do IM8 para o Murex Uruguai, seguindo o *Guia de Implementação v1.0*, a documentação da API de curvas e o **mesmo padrão da `obterdadospreco`**: `requests` (HTTP com retry), `loguru` (logs em JSON) e `boto3` (Secrets Manager e SQS), com os serviços `credentials_service`, `sts_service` e `im8_..._service`.

**Fluxo:** evento do Connector → valida a curva → credenciais (`InfraSecretsMX3`) → token (STS) → tipo pelo `cadastrocurvas` (se faltar) → pontos na API de curvas → filtra os pilares → envia curva + pilares para a SQS de curvas (Lambda de gravar dados).

**Instalar:** `pip install -r requirements.txt` (o `boto3` já vem no runtime da Lambda).

**Teste local:** `python teste_local.py` (simulado; gera `mensagens_sqs.json` para a Lambda de gravar) e `python teste_local.py rede`. Se o pip estiver bloqueado, o teste usa substitutos locais e roda mesmo assim.

**Status:** pronta e testada localmente com dados simulados. A rede de dev já alcança o STS e a API de curvas. Falta o clientID das curvas para a chamada real e a fila de curvas.

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
| `FILA_CURVAS_URL` | URL da SQS de curvas (vazia = não envia, só registra no log) |
| `X_APIGW_API_ID` | vazio (a API de curvas não usa esse cabeçalho) |
| `CAMPO_INTERPOLADO` | opcional; padrão `indicador_interpolado` (confirmar com resposta real) |
| `LOG_JSON` | `true` na AWS (padrão); `false` deixa o log legível no teste local |

## Arquivos

### `requirements.txt`

Dependências: `requests` e `loguru` (mesmas da obterdadospreco).

```text
# Lambda de captura de curvas (obterdadoscurvas) — mesmo padrão da obterdadospreco
requests>=2.31
loguru>=0.7
# boto3 já vem no runtime Python da AWS Lambda (não precisa empacotar).
# Para testar fora da AWS com chamadas reais ao Secrets Manager / SQS, instale também:
# boto3>=1.28
```

### `lambda_function.py`

Handler (`lambda_function.lambda_handler`): orquestra as etapas, monta a mensagem (correlation_id + curva + pilares) e envia para a fila.

```python
"""
obterdadoscurvas — Lambda de captura de curvas do IM8 (mesmo padrão da obterdadospreco).

Fluxo: evento do Connector -> valida a curva -> credenciais (InfraSecretsMX3) -> token (STS)
       -> tipo pelo cadastrocurvas (se faltar) -> pontos na API de curvas -> filtra os pilares
       -> envia curva + pilares para a SQS de curvas (Lambda de gravar dados).
"""
import uuid

from credentials_service import obtain_client_credentials
from curva_event_model import CurvaEventModel, EventoInvalido
from http_request_error import HttpRequestError
from im8_curvas_service import Im8CurvasService
from logger_config import logger
from pilares_service import filtrar_pilares
from sqs_service import publicar_mensagem
from sts_service import StsService


def lambda_handler(event, context):
    request_id = getattr(context, "aws_request_id", "local")
    correlation_id = str(uuid.uuid1())
    logger.configure(extra={"correlation_id": correlation_id, "aws_request_id": request_id})
    registros = event if isinstance(event, list) else [event]
    logger.info("Início da lambda ObterDadosCurvas.", total_eventos=len(registros))

    credentials_model = obtain_client_credentials()
    sts_token = StsService().obtain_sts_token(credentials_model)
    im8_service = Im8CurvasService(correlation_id, credentials_model.client_id)

    resultados = []
    for elemento in registros:
        try:
            curva = CurvaEventModel.from_event(elemento["payload"]["value"])
        except (EventoInvalido, KeyError, TypeError) as e:
            logger.error("Evento de curva inválido, descartado.", erro=str(e))
            resultados.append({"status": "INVALIDO", "erro": str(e)})
            continue

        try:
            if not curva.tipo_curva:
                curva.tipo_curva = im8_service.obter_tipo_cadastro(sts_token, curva)
            resposta, url = im8_service.obter_pontos(sts_token, curva)
        except (HttpRequestError, ValueError) as e:
            logger.error("Falha ao obter pontos da curva.", codigo_curva=curva.codigo_curva, erro=str(e))
            resultados.append({"codigo_curva": curva.codigo_curva, "status": "ERRO", "erro": str(e)})
            continue

        pontos, pilares, campo = filtrar_pilares(resposta)
        primeiro = pontos[0] if pontos and isinstance(pontos[0], dict) else {}
        logger.info("Curva capturada.", codigo_curva=curva.codigo_curva, total_pontos=len(pontos),
                    total_pilares=len(pilares))

        # Mensagem para a Lambda de gravar dados (contrato: correlation_id + curva + pilares).
        mensagem = {"correlation_id": correlation_id, "curva": curva.to_dict(), "pilares": pilares}
        enviada = publicar_mensagem(mensagem) if pilares else False
        if not pilares:
            logger.error("Curva sem pilares: não enviada para gravação.", codigo_curva=curva.codigo_curva)

        resultados.append({
            "codigo_curva": curva.codigo_curva, "nome_curva": curva.nome_curva, "tipo_curva": curva.tipo_curva,
            "status": "CAPTURADA" if pilares else "SEM_PILARES", "url_chamada": url,
            "total_pontos": len(pontos), "total_pilares": len(pilares), "enviada_fila": enviada,
            # diagnóstico para confirmar o formato real da resposta:
            "campos_de_cada_ponto": list(primeiro.keys()), "campo_interpolado_existe": campo in primeiro,
            "mensagem_sqs": mensagem,
        })

    logger.info("Término.", total=len(resultados))
    return {"correlation_id": correlation_id, "resultados": resultados}
```

### `logger_config.py`

Logger `loguru` em JSON (`serialize=True`) com fuso de São Paulo, igual ao da obterdadospreco.

```python
import os
import sys
import time
from sys import platform

from loguru import logger

if platform == "linux" or platform == "linux2":
    os.environ['TZ'] = 'America/Sao_Paulo'
    time.tzset()

logger.remove()
format_template = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level>\
 | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level> | {extra}"

# Na AWS sai em JSON (serialize=True, igual à obterdadospreco). No teste local, LOG_JSON=false deixa legível.
logger.add(sink=sys.stderr, backtrace=False, diagnose=False, format=format_template,
           serialize=os.getenv("LOG_JSON", "true").lower() == "true")
```

### `http_request_error.py`

Erro padrão das chamadas HTTP.

```python
class HttpRequestError(Exception):
    def __init__(self, message, status_code, url_called):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.url_called = url_called
```

### `credentials_model.py`

Modelo com client_id e client_secret.

```python
from dataclasses import dataclass


@dataclass
class CredentialsModel:
    client_id: str
    client_secret: str
```

### `credentials_service.py`

Lê as credenciais no Secrets Manager (`InfraSecretsMX3`) com `boto3`.

```python
import json
import os

import boto3
from botocore.exceptions import ClientError

from credentials_model import CredentialsModel
from logger_config import logger


def obtain_client_credentials():
    """Mesmo padrão da obterdadospreco: segredo compartilhado (SECRET_MANAGER_NAME, ex.: InfraSecretsMX3)
    com uma chave por lambda. Os NOMES das chaves vêm de AWS_LAMBDA_CLIENT_ID / AWS_LAMBDA_SECRET_ID."""
    aws_client_id = os.getenv("AWS_LAMBDA_CLIENT_ID")
    aws_secret_id = os.getenv("AWS_LAMBDA_SECRET_ID")
    region_name = os.getenv("AWS_REGION_NAME")
    secret_name = os.getenv("SECRET_MANAGER_NAME")

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        secret_value_response = client.get_secret_value(SecretId=secret_name)
        secrets = json.loads(secret_value_response["SecretString"])
        return CredentialsModel(secrets[aws_client_id], secrets[aws_secret_id])
    except ClientError as e:
        logger.exception("Ocorreu um ClientError ao buscar as credenciais no secrets manager.", secret_name=secret_name)
        raise e
    except KeyError as e:
        logger.error("Chave não encontrada no secret.", secret_name=secret_name, chave=str(e))
        raise e
    except Exception as e:
        logger.exception("Ocorreu um erro inesperado durante a recuperação das credenciais.", secret_name=secret_name)
        raise e
```

### `sts_service.py`

Obtém o token no STS com `requests` + retry, mesmo protocolo da obterdadospreco.

```python
import os

import requests
from requests.adapters import HTTPAdapter, Retry

from http_request_error import HttpRequestError
from logger_config import logger


class StsService:

    def __init__(self):
        self.url_base = os.getenv("URLSts")
        self.certificate_path = os.getenv("CA_CERT_PATH")

    def obtain_sts_token(self, credentials_model):
        request_path = self.url_base.rstrip("/") + "/api/oauth/token"
        try:
            session = requests.Session()
            retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504], allowed_methods={'GET'})
            session.mount('https://', HTTPAdapter(max_retries=retries))

            data = {
                'grant_type': 'client_credentials',
                'client_id': credentials_model.client_id,
                'client_secret': credentials_model.client_secret
            }
            response = session.get(data=data, url=request_path, verify=self.certificate_path, timeout=10)

            if response.status_code == 200:
                return response.json()['access_token']

            logger.error("Falha no sts request. Não foi possível obter o token.",
                         status_code_error=response.status_code, error_message=response.reason,
                         url_called=response.url)
            raise HttpRequestError(message=response.reason, status_code=response.status_code,
                                   url_called=response.url)
        except HttpRequestError:
            raise
        except Exception as e:
            logger.exception("Ocorreu um erro inesperado durante a recuperação do token jwt do sts.")
            raise e
```

### `curva_event_model.py`

Modelo e validação do evento de curva (`payload.value`). `nome_tipo_curva` é opcional.

```python
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

TIPOS_VALIDOS = ("BASE", "COMPOSTA", "TRANSFER")


class EventoInvalido(Exception):
    pass


@dataclass
class CurvaEventModel:
    """Evento de curva (payload.value do Connector). nome_tipo_curva é opcional:
    se faltar, o tipo é resolvido pelo cadastrocurvas (guia, 6.4)."""
    codigo_curva: str
    nome_curva: str
    tipo_curva: Optional[str]
    data_liberacao: str      # yyyymmdd
    data_inclusao: str

    @classmethod
    def from_event(cls, value):
        for campo in ("codigo_curva", "nome_curva", "data_liberacao", "data_inclusao"):
            if not str(value.get(campo) or "").strip():
                raise EventoInvalido(f"campo obrigatório ausente: {campo}")
        tipo = str(value.get("nome_tipo_curva") or "").strip().upper() or None
        if tipo and tipo not in TIPOS_VALIDOS:
            raise EventoInvalido(f"nome_tipo_curva desconhecido: {tipo}")
        return cls(codigo_curva=str(value["codigo_curva"]).strip(),
                   nome_curva=str(value["nome_curva"]).strip(),
                   tipo_curva=tipo,
                   data_liberacao=_para_yyyymmdd(str(value["data_liberacao"]).strip()),
                   data_inclusao=str(value["data_inclusao"]).strip())

    def to_dict(self):
        return asdict(self)


def _para_yyyymmdd(texto):
    if re.fullmatch(r"\d{8}", texto):
        return texto
    if re.match(r"\d{4}-\d{2}-\d{2}", texto):
        return datetime.strptime(texto[:10], "%Y-%m-%d").strftime("%Y%m%d")
    raise EventoInvalido(f"data_liberacao inválida: {texto}")
```

### `im8_curvas_service.py`

Chama a API de curvas com `requests` + retry: `cadastrocurvas` (tipo em `vigencias[].desTipoCurv`), `pontoscurvas` (BASE/COMPOSTA) e `pontotransferencias` (TRANSFER).

```python
import os
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter, Retry

from curva_event_model import TIPOS_VALIDOS
from http_request_error import HttpRequestError
from logger_config import logger


class Im8CurvasService:
    """API de curvas do IM8, no mesmo padrão do im8_service.py da obterdadospreco.
    Cabeçalhos da documentação da API de curvas: x-itau-apikey, Accept-Encoding gzip, x-itau-correlationID e
    Authorization. x-apigw-api-id NÃO é usado na API de curvas (só é enviado se a variável for preenchida).
    A URL é montada pela configuração (URL_IM8_CURVAS): o campo `url` do evento nunca é chamado."""

    def __init__(self, correlation_id, client_id):
        self.base_url = os.getenv("URL_IM8_CURVAS").rstrip("/") + "/curvas/v1"
        self.x_itau_apikey = client_id
        self.x_itau_correlation_id = correlation_id
        self.x_apigw_api_id = os.getenv("X_APIGW_API_ID")
        self.certificate_path = os.getenv("CA_CERT_PATH")

    def obter_tipo_cadastro(self, sts_token, curva):
        """Guia 6.4: tipo pelo cadastrocurvas (última versão). Resposta: vigencias[].desTipoCurv."""
        cadastro = self._get(sts_token, f"{self.base_url}/cadastrocurvas",
                             {"NomCurv": curva.nome_curva, "ApenasUltimaVersao": "true"}, curva, "cadastrocurvas")
        registros = cadastro if isinstance(cadastro, list) else [cadastro]
        vigencias = [v for r in registros if isinstance(r, dict) for v in (r.get("vigencias") or [])
                     if isinstance(v, dict)]
        vigencias.sort(key=lambda v: not v.get("indUltiVers"))     # última versão primeiro
        for vigencia in vigencias:
            tipo = str(vigencia.get("desTipoCurv") or "").strip().upper()
            if tipo in TIPOS_VALIDOS:
                base = vigencia.get("curvaBase") or {}
                logger.info("Cadastro da curva.", codigo_curva=curva.codigo_curva, tipo_curva=tipo,
                            versao=vigencia.get("numVersCurv"), vertice=(base.get("vertice") or {}).get("nome"),
                            prazo=(base.get("prazo") or {}).get("nome"))
                return tipo
        raise ValueError("tipo da curva (desTipoCurv) não encontrado no cadastrocurvas")

    def obter_pontos(self, sts_token, curva):
        """TRANSFER -> pontotransferencias (pelo código); BASE/COMPOSTA -> pontoscurvas (pelo nome)."""
        if curva.tipo_curva == "TRANSFER":
            url = f"{self.base_url}/pontotransferencias"
            params = {"codigocurva": curva.codigo_curva, "dataexecucaocurva": curva.data_liberacao}
        else:
            url = f"{self.base_url}/pontoscurvas/{quote(curva.nome_curva, safe='')}"
            params = {"data_execucao": curva.data_liberacao}
        return self._get(sts_token, url, params, curva, "pontos"), url

    def _get(self, sts_token, url, params, curva, nome_chamada):
        try:
            session = requests.Session()
            retries = Retry(total=6, backoff_factor=1, status_forcelist=[502, 503, 504], allowed_methods={'GET'})
            session.mount('https://', HTTPAdapter(max_retries=retries))

            headers = {
                "x-itau-apikey": self.x_itau_apikey,
                "x-itau-correlationID": self.x_itau_correlation_id,
                "Authorization": "Bearer " + sts_token,
                "Accept-Encoding": "gzip",          # requests descomprime automaticamente
            }
            if self.x_apigw_api_id:
                headers["x-apigw-api-id"] = self.x_apigw_api_id

            response = session.get(url=url, params=params, headers=headers, verify=self.certificate_path, timeout=15)
            logger.info(f"Resposta da API {nome_chamada} do IM8.", codigo_curva=curva.codigo_curva,
                        status=response.status_code, url_called=response.url,
                        tamanho_bytes=len(response.content or b""))

            if response.status_code == 200:
                return response.json()

            logger.error(f"Falha na chamada da api {nome_chamada} do IM8.", status_code_error=response.status_code,
                         error_message=response.reason, url_called=response.url)
            raise HttpRequestError(message=response.reason, status_code=response.status_code,
                                   url_called=response.url)
        except HttpRequestError:
            raise
        except Exception as e:
            logger.exception(f"Erro inesperado na chamada da api {nome_chamada} do IM8.", url=url)
            raise e
```

### `pilares_service.py`

Filtra os pontos interpolados e mantém só os pilares.

```python
import os

from logger_config import logger


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
    logger.info("Pilares filtrados.", total_pontos=len(pontos), total_pilares=len(pilares), campo_interpolado=campo)
    return pontos, pilares, campo
```

### `sqs_service.py`

Envia a mensagem para a SQS de curvas com `boto3`.

```python
import json
import os

import boto3

from logger_config import logger


def publicar_mensagem(mensagem):
    """Envia a mensagem (curva + pilares) para a SQS de curvas, que aciona a Lambda de gravar dados.
    Se FILA_CURVAS_URL não estiver configurada (teste local / fila ainda não criada), só registra no log."""
    url_fila = os.getenv("FILA_CURVAS_URL")
    if not url_fila:
        logger.info("Fila de curvas não configurada: mensagem não enviada.",
                    codigo_curva=mensagem["curva"]["codigo_curva"])
        return False

    sqs = boto3.client("sqs", region_name=os.getenv("AWS_REGION_NAME", "sa-east-1"))
    sqs.send_message(QueueUrl=url_fila, MessageBody=json.dumps(mensagem, ensure_ascii=False, default=str),
                     MessageAttributes={"correlationId": {"DataType": "String",
                                                          "StringValue": mensagem["correlation_id"]}})
    logger.info("Mensagem enviada para a fila de curvas.", codigo_curva=mensagem["curva"]["codigo_curva"],
                total_pilares=len(mensagem["pilares"]))
    return True
```

### `teste_local.py`

Teste local sem AWS (modo simulado e modo `rede`), com substitutos se as bibliotecas não estiverem instaladas.

```python
"""
Teste local da captura de curvas, sem AWS.

  python teste_local.py            -> SIMULADO: roda o código inteiro com STS e IM8 de mentira
                                      e salva mensagens_sqs.json para a Lambda de gravar dados
  python teste_local.py rede       -> REDE: só verifica se a máquina alcança o STS e a API de curvas
  python teste_local.py rede URL   -> REDE testando também outra URL base da API de curvas

Instale as bibliotecas com:  pip install -r requirements.txt
Se o pip estiver bloqueado, o teste roda mesmo assim usando substitutos locais.
Nenhum modo usa credencial nem grava na AWS.
"""
import json
import os
import socket
import ssl
import sys
import urllib.error
import urllib.request
from types import SimpleNamespace
from unittest.mock import patch

PASTA = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PASTA)
os.environ.setdefault("LOG_JSON", "false")

# ---------------------------------------------------------------------------------------------
# Substitutos para rodar o TESTE LOCAL numa máquina sem pip (loguru / requests / boto3).
# Só entram se a biblioteca real NÃO estiver instalada. Não fazem parte da Lambda na AWS.
# ---------------------------------------------------------------------------------------------
import sys
import types


def _instalar_substitutos():
    try:
        import loguru  # noqa: F401
    except ImportError:
        class _Logger:
            def _log(self, nivel, mensagem, **campos):
                extras = " ".join(f"{k}={v}" for k, v in campos.items())
                print(f"{nivel:<8} | {mensagem} {extras}".rstrip())

            def info(self, mensagem, *a, **k): self._log("INFO", mensagem, **k)
            def warning(self, mensagem, *a, **k): self._log("WARNING", mensagem, **k)
            def error(self, mensagem, *a, **k): self._log("ERROR", mensagem, **k)
            def exception(self, mensagem, *a, **k): self._log("ERROR", mensagem, **k)
            def debug(self, *a, **k): pass
            def remove(self, *a, **k): pass
            def add(self, *a, **k): pass
            def configure(self, *a, **k): pass

        mod = types.ModuleType("loguru")
        mod.logger = _Logger()
        sys.modules["loguru"] = mod
        print("[aviso] loguru não instalado: usando substituto local.")

    try:
        import requests  # noqa: F401
    except ImportError:
        req = types.ModuleType("requests")
        adapters = types.ModuleType("requests.adapters")

        class Session:
            def mount(self, *a, **k): pass
            def get(self, *a, **k): raise RuntimeError("requests não instalado: chamada real indisponível")

        class HTTPAdapter:
            def __init__(self, *a, **k): pass

        class Retry:
            def __init__(self, *a, **k): pass

        req.Session, adapters.HTTPAdapter, adapters.Retry, req.adapters = Session, HTTPAdapter, Retry, adapters
        sys.modules.update({"requests": req, "requests.adapters": adapters})
        print("[aviso] requests não instalado: usando substituto local.")

    try:
        import boto3  # noqa: F401
    except ImportError:
        boto3_mod = types.ModuleType("boto3")
        sessao = types.ModuleType("boto3.session")
        botocore = types.ModuleType("botocore")
        excecoes = types.ModuleType("botocore.exceptions")

        class _Session:
            def client(self, *a, **k): raise RuntimeError("boto3 não instalado: AWS indisponível")

        class ClientError(Exception):
            pass

        sessao.Session = _Session
        boto3_mod.session = sessao
        boto3_mod.client = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boto3 não instalado"))
        excecoes.ClientError = ClientError
        botocore.exceptions = excecoes
        sys.modules.update({"boto3": boto3_mod, "boto3.session": sessao,
                            "botocore": botocore, "botocore.exceptions": excecoes})
        print("[aviso] boto3 não instalado: usando substituto local.")


_instalar_substitutos()
# ---------------------------------------------------------------------------------------------

URL_STS = "https://des-sts-int.mbi.cloud.ihf"
URL_CURVAS_PADRAO = "https://curvas-v1-distribuiprecoscorporativos.api-sp.dev.aws.cloud.ihf"


# --------------------------------------------------------------------------- modo SIMULADO
def modo_simulado():
    os.environ.update(SECRET_MANAGER_NAME="InfraSecretsMX3", AWS_LAMBDA_CLIENT_ID="CHAVE_CLIENT_ID",
                      AWS_LAMBDA_SECRET_ID="CHAVE_SECRET", URLSts=URL_STS, URL_IM8_CURVAS=URL_CURVAS_PADRAO)
    os.environ.pop("FILA_CURVAS_URL", None)
    import im8_curvas_service
    import lambda_function
    import sts_service

    def get_falso(self, sts_token, url, params, curva, nome_chamada):
        print(f"   -> chamaria: GET {url}  params={params}")
        if nome_chamada == "cadastrocurvas":
            return {"codCurv": curva.codigo_curva, "nomCurv": curva.nome_curva,
                    "vigencias": [{"indUltiVers": True, "desTipoCurv": "BASE",
                                   "curvaBase": {"vertice": {"nome": "PU"}, "prazo": {"nome": "DC - 360"}}}]}
        return {"pontos": [
            {"prazo_dias": 1, "taxa": "10.65", "indicador_interpolado": ""},
            {"prazo_dias": 45, "taxa": "10.74", "indicador_interpolado": "S"},
            {"prazo_dias": 90, "taxa": "10.81", "indicador_interpolado": ""},
            {"prazo_dias": 135, "taxa": "10.86", "indicador_interpolado": "S"},
            {"prazo_dias": 180, "taxa": "10.93", "indicador_interpolado": ""},
        ]}

    evento_arq = next((f for f in ("evento_teste_3_curvas.json", "evento_teste_console.json")
                       if os.path.exists(os.path.join(PASTA, f))), None)
    if not evento_arq:
        print("Coloque evento_teste_3_curvas.json ou evento_teste_console.json nesta pasta.")
        return
    with open(os.path.join(PASTA, evento_arq), encoding="utf-8") as f:
        evento = json.load(f)

    print(f"\n=== SIMULADO (evento: {evento_arq}) - STS e IM8 de mentira ===\n")
    credenciais = SimpleNamespace(client_id="client-teste", client_secret="secret-teste")
    with patch.object(lambda_function, "obtain_client_credentials", return_value=credenciais), \
         patch.object(sts_service.StsService, "obtain_sts_token", return_value="token-de-teste"), \
         patch.object(im8_curvas_service.Im8CurvasService, "_get", get_falso):
        resultado = lambda_function.lambda_handler(evento, None)

    print("\n=== RESULTADO (o que a Lambda devolveria) ===")
    print(json.dumps(resultado, indent=2, ensure_ascii=False))

    mensagens = [r["mensagem_sqs"] for r in resultado["resultados"] if r.get("status") == "CAPTURADA"]
    destino = os.path.join(PASTA, "mensagens_sqs.json")
    with open(destino, "w", encoding="utf-8") as f:
        json.dump(mensagens, f, indent=2, ensure_ascii=False)
    print(f"\n{len(mensagens)} mensagem(ns) para a Lambda de gravar dados salvas em: {destino}")


# --------------------------------------------------------------------------- modo REDE
def testar_endereco(nome, url):
    print(f"\n{nome}: {url}")
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=10) as r:
            print(f"   ALCANÇOU. Respondeu HTTP {r.status}.")
    except urllib.error.HTTPError as e:
        print(f"   ALCANÇOU. Respondeu HTTP {e.code} (normal sem login: a rede chega lá).")
    except urllib.error.URLError as e:
        motivo = e.reason
        if isinstance(motivo, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in str(motivo):
            print("   ALCANÇOU, mas o Python não confia no certificado do banco.")
        elif isinstance(motivo, socket.gaierror):
            print("   NÃO ALCANÇOU: o nome não foi encontrado (DNS).")
        elif "timed out" in str(motivo):
            print("   NÃO ALCANÇOU: tempo esgotado (bloqueio de rede/firewall).")
        else:
            print(f"   NÃO ALCANÇOU: {type(motivo).__name__}: {motivo}")
    except (socket.timeout, TimeoutError):
        print("   NÃO ALCANÇOU: tempo esgotado (bloqueio de rede/firewall).")
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
