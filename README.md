# Captura de Curvas IM8 → Murex Uruguai

Versões mais recentes das duas Lambdas de curvas, no **mesmo padrão do fluxo de preços** (`obterdadospreco` / `enviardadospreco`): `requests`, `loguru` e `boto3`. Cada Lambda fica numa pasta própria, sem subpastas.

```
IM8 (Kafka) → [obterdadoscurvas] → SQS de curvas → [gravardadoscurvas] → XML → Murex
                 token STS + API de curvas            template S3 (dadoslambda)
                 + filtro de pilares                  + gravação atômica
```

| Lambda | Pasta | O que faz |
|---|---|---|
| Captura (Obter Dados Curvas) | `testeaws` / obterdadoscurvas | Recebe o evento, autentica no STS, chama a API de curvas do IM8, filtra os pilares e envia para a fila |
| Gravar Dados Murex Uruguai (curvas) | `gravardadoscurvas` | Lê a fila, monta o XML pelo template e grava no destino |

**Teste do fluxo completo (local, sem AWS):**
```
cd testeaws
pip install -r requirements.txt
python teste_local.py
cd ..\gravardadoscurvas
pip install -r requirements.txt
python teste_local.py ..\testeaws\mensagens_sqs.json
```
Se o `pip` estiver bloqueado, os testes rodam mesmo assim com substitutos locais.

**Status:** fluxo completo testado localmente com dados simulados. A rede de dev já alcança o STS e a API de curvas. Pendências: credencial de consumidor da API de curvas (chamado no Shared – Preços e Cálculos), contrato XML/XSD de curva do Murex, fila de curvas e destino final.

---

## Captura de Curvas IM8 (`testeaws` / obterdadoscurvas)

Lambda de captura de curvas do IM8 para o Murex Uruguai, seguindo o *Guia de Implementação v1.0*, a documentação da API de curvas e o **mesmo padrão da `obterdadospreco`**: `requests` (HTTP com retry), `loguru` (logs em JSON) e `boto3` (Secrets Manager e SQS), com os serviços `credentials_service`, `sts_service` e `im8_..._service`.

**Fluxo:** evento do Connector → valida a curva → credenciais (`InfraSecretsMX3`) → token (STS) → tipo pelo `cadastrocurvas` (se faltar) → pontos na API de curvas → filtra os pilares → envia curva + pilares para a SQS de curvas (Lambda de gravar dados).

**Instalar:** `pip install -r requirements.txt` (o `boto3` já vem no runtime da Lambda).

**Teste local:** `python teste_local.py` (simulado; gera `mensagens_sqs.json` para a Lambda de gravar) e `python teste_local.py rede`. Se o pip estiver bloqueado, o teste usa substitutos locais e roda mesmo assim.

**Status:** pronta e testada localmente com dados simulados. A rede de dev já alcança o STS e a API de curvas. Falta o clientID das curvas para a chamada real e a fila de curvas.

### Variáveis de ambiente

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

### Arquivos

#### `requirements.txt`

Dependências: `requests` e `loguru` (mesmas da obterdadospreco).

```text
# Lambda de captura de curvas (obterdadoscurvas) — mesmo padrão da obterdadospreco
requests>=2.31
loguru>=0.7
# boto3 já vem no runtime Python da AWS Lambda (não precisa empacotar).
# Para testar fora da AWS com chamadas reais ao Secrets Manager / SQS, instale também:
# boto3>=1.28
```

#### `lambda_function.py`

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

#### `logger_config.py`

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

#### `http_request_error.py`

Erro padrão das chamadas HTTP.

```python
class HttpRequestError(Exception):
    def __init__(self, message, status_code, url_called):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.url_called = url_called
```

#### `credentials_model.py`

Modelo com client_id e client_secret.

```python
from dataclasses import dataclass


@dataclass
class CredentialsModel:
    client_id: str
    client_secret: str
```

#### `credentials_service.py`

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

#### `sts_service.py`

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

#### `curva_event_model.py`

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

#### `im8_curvas_service.py`

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

#### `pilares_service.py`

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

#### `sqs_service.py`

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

#### `teste_local.py`

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

#### `evento_teste_console.json`

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

#### `evento_teste_3_curvas.json`

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

---

## Gravar Dados Murex Uruguai — Curvas (`gravardadoscurvas`)

Lambda que transforma as curvas capturadas no IM8 no arquivo XML do Murex, seguindo o *Guia de Implementação v1.0* (seção 8) e o **padrão da `enviardadospreco`**: template no S3 com a palavra-chave `dadoslambda`, `loguru` (logs em JSON) e `boto3` (S3).

**Fluxo:** mensagem da SQS (enviada pela captura) → valida → template XML (S3) → substitui `dadoslambda` pelos pilares → valida o XML → grava de forma atômica no destino.

**Instalar:** `pip install -r requirements.txt` (o `boto3` já vem no runtime da Lambda).

**Teste local:** `python teste_local.py`, `python teste_local.py fator` e `python teste_local.py ..\testeaws\mensagens_sqs.json` (fluxo completo). Se o pip estiver bloqueado, o teste usa substitutos locais.

**Status:** ligada à captura e testada de ponta a ponta com dados simulados. Provisórios: tags de curva do XML (contrato Murex), nomes dos campos dos pilares (resposta real do IM8) e destino final (EFS ou API on-premises).

### Variáveis de ambiente

| Chave | Valor |
|---|---|
| `DESTINO` | `local` (teste), `s3` ou `efs` |
| `SAIDA_DIR` | pasta de saída para `local`/`efs` (padrão: `saida`) |
| `SAIDA_BUCKET` / `SAIDA_PREFIX` | bucket e prefixo quando `DESTINO=s3` |
| `TEMPLATES_BUCKET` | bucket dos templates (vazio = usa o arquivo local) |
| `TEMPLATES_PREFIX` | padrão `templates-xml-murex/` |
| `TEMPLATE_CURVA` | padrão `curva_template.xml` |
| `TEMPLATE_VERSAO` | versão no nome do arquivo (padrão `v1`) |
| `CAMPO_PRAZO` / `CAMPO_TAXA` / `CAMPO_PU` | nomes dos campos dos pilares (padrão `prazo_dias`, `taxa`, `pu`) |
| `CALCULAR_FATOR` | `true` para calcular Fator = 100000 / PU (padrão `false`) |
| `AWS_REGION_NAME` | `sa-east-1` |
| `LOG_JSON` | `true` na AWS (padrão); `false` deixa o log legível no teste local |

### Arquivos

#### `requirements.txt`

Dependências: `loguru` (o `boto3` já vem no runtime da Lambda).

```text
# Lambda de gravar dados de curvas (gravardadoscurvas) — mesmo padrão da enviardadospreco
loguru>=0.7
# boto3 já vem no runtime Python da AWS Lambda (não precisa empacotar).
# Para testar fora da AWS lendo templates / gravando no S3, instale também:
# boto3>=1.28
```

#### `lambda_function.py`

Handler: recebe o lote da SQS e devolve em `batchItemFailures` só as mensagens que falharam (voltam para a fila e, esgotadas as tentativas, vão para a DLQ).

```python
"""
gravardadoscurvas — Lambda "Gravar Dados Murex Uruguai" para curvas (guia, seções 3.9 a 3.12 e 8).

Fluxo: mensagem da SQS -> valida -> template XML (S3) -> substitui 'dadoslambda' pelos pilares
       -> valida o XML -> grava de forma atômica no destino (local / S3 / EFS).
Bibliotecas no mesmo padrão da enviardadospreco: loguru (logs) e boto3 (S3).
"""
from entrega_service import ErroEntrega, entregar, nome_arquivo
from logger_config import logger
from mensagem_model import MensagemInvalida, ler_mensagem
from template_service import obter_template
from xml_curva_service import ErroTransformacao, gerar_xml


def processar(body):
    correlation_id, curva, pilares = ler_mensagem(body)
    template, origem = obter_template()
    conteudo = gerar_xml(template, curva, pilares)
    caminho, tamanho, hash_arquivo = entregar(nome_arquivo(curva), conteudo)
    logger.info("XML da curva entregue.", correlation_id=correlation_id, codigo_curva=curva["codigo_curva"],
        nome_curva=curva["nome_curva"], total_pilares=len(pilares), template=origem,
        arquivo=caminho, tamanho_bytes=tamanho, hash=hash_arquivo)
    return {"codigo_curva": curva["codigo_curva"], "status": "ENTREGUE", "arquivo": caminho,
            "tamanho_bytes": tamanho, "hash": hash_arquivo}


def lambda_handler(event, context):
    """Gatilho SQS com ReportBatchItemFailures: só as mensagens com erro voltam para a fila
    (e vão para a DLQ quando esgotarem as tentativas)."""
    logger.configure(extra={"aws_request_id": getattr(context, "aws_request_id", "local")})
    registros = event.get("Records", [])
    logger.info("Início da lambda GravarDadosCurvas.", total_mensagens=len(registros))

    falhas, resultados = [], []
    for registro in registros:
        message_id = registro.get("messageId")
        try:
            resultados.append(processar(registro.get("body")))
        except (MensagemInvalida, ErroTransformacao, ErroEntrega) as e:
            logger.error("Falha ao gravar curva.", message_id=message_id, tipo_erro=type(e).__name__, erro=str(e))
            falhas.append({"itemIdentifier": message_id})
            resultados.append({"message_id": message_id, "status": "ERRO", "erro": str(e)})
        except Exception as e:
            logger.error("Erro inesperado ao gravar curva.", message_id=message_id, erro=repr(e)[:300])
            falhas.append({"itemIdentifier": message_id})
            resultados.append({"message_id": message_id, "status": "ERRO", "erro": repr(e)[:300]})

    logger.info("Término.", total_ok=len(registros) - len(falhas), total_erro=len(falhas))
    return {"batchItemFailures": falhas, "resultados": resultados}
```

#### `logger_config.py`

Logger `loguru` em JSON, igual ao da captura e ao da obterdadospreco.

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

#### `mensagem_model.py`

Valida a mensagem que a captura envia: bloco `curva` com os campos obrigatórios e lista de `pilares`.

```python
import json


class MensagemInvalida(Exception):
    pass


def ler_mensagem(body):
    """Mensagem que a captura publica na SQS de curvas:
    {"correlation_id": "...", "curva": {codigo_curva, nome_curva, tipo_curva, data_liberacao, data_inclusao},
     "pilares": [ {...}, ... ]}
    """
    try:
        mensagem = json.loads(body) if isinstance(body, str) else body
    except ValueError:
        raise MensagemInvalida("corpo da mensagem não é JSON")
    if not isinstance(mensagem, dict):
        raise MensagemInvalida("mensagem não é um objeto JSON")

    curva = mensagem.get("curva")
    if not isinstance(curva, dict):
        raise MensagemInvalida("mensagem sem o bloco 'curva'")
    for campo in ("codigo_curva", "nome_curva", "data_liberacao", "data_inclusao"):
        if not str(curva.get(campo) or "").strip():
            raise MensagemInvalida(f"curva sem o campo obrigatório: {campo}")

    pilares = mensagem.get("pilares")
    if not isinstance(pilares, list) or not pilares:
        raise MensagemInvalida("mensagem sem pilares")
    return mensagem.get("correlation_id") or "sem-correlation-id", curva, pilares
```

#### `template_service.py`

Lê o template XML do S3 com `boto3` (`TEMPLATES_BUCKET`) ou do arquivo local no teste.

```python
import os

import boto3

from logger_config import logger

PALAVRA_CHAVE = "dadoslambda"   # mesma convenção dos templates de preço (enviardadospreco)
_cache = {}


def obter_template():
    """Lê o template XML da curva.
    - Na AWS: bucket TEMPLATES_BUCKET, chave TEMPLATES_PREFIX + TEMPLATE_CURVA
      (como TEMPLATES_MUREX_S3_KEY = templates-xml-murex/ na enviardadospreco).
    - Local: arquivo TEMPLATE_LOCAL (padrão curva_template.xml nesta pasta).
    """
    bucket = os.getenv("TEMPLATES_BUCKET")
    if bucket:
        chave = os.getenv("TEMPLATES_PREFIX", "templates-xml-murex/") + os.getenv("TEMPLATE_CURVA", "curva_template.xml")
        if chave not in _cache:
            try:
                s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION_NAME", "sa-east-1"))
                _cache[chave] = s3.get_object(Bucket=bucket, Key=chave)["Body"].read().decode("utf-8")
            except Exception as e:
                logger.exception("Erro ao ler o template no S3.", bucket=bucket, chave=chave)
                raise e
        return _cache[chave], f"s3://{bucket}/{chave}"

    caminho = os.getenv("TEMPLATE_LOCAL") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "curva_template.xml")
    with open(caminho, encoding="utf-8") as f:
        return f.read(), caminho
```

#### `xml_curva_service.py`

Monta o bloco da curva (pilares ordenados por prazo), calcula Fator = 100000 / PU quando ligado (PU = 0 é erro), substitui `dadoslambda` e valida o XML.

```python
import os
import xml.dom.minidom
from decimal import Decimal, InvalidOperation
from xml.sax.saxutils import quoteattr

from template_service import PALAVRA_CHAVE


class ErroTransformacao(Exception):
    pass


def _numero(valor, nome):
    try:
        return Decimal(str(valor).replace(",", "."))
    except (InvalidOperation, ValueError):
        raise ErroTransformacao(f"valor não numérico em '{nome}': {valor!r}")


def calcular_fator(pu):
    """Guia 6.3: Fator = 100000 / PU. PU = 0 é erro funcional, nunca divisão automática."""
    pu = _numero(pu, "PU")
    if pu == 0:
        raise ErroTransformacao("PU igual a zero com cálculo de fator exigido")
    return Decimal("100000") / pu


def montar_bloco_curva(curva, pilares):
    """Bloco que substitui 'dadoslambda'. PROVISÓRIO: as tags dependem do contrato XML/XSD do Murex (pendência 15).
    Os nomes dos campos dos pilares vêm de variáveis, porque o formato real da API ainda não foi visto."""
    campo_prazo = os.getenv("CAMPO_PRAZO", "prazo_dias")
    campo_taxa = os.getenv("CAMPO_TAXA", "taxa")
    campo_pu = os.getenv("CAMPO_PU", "pu")
    usar_fator = os.getenv("CALCULAR_FATOR", "false").lower() == "true"

    linhas = [f"<mp:curve mp:name={quoteattr(curva['nome_curva'])} mp:code={quoteattr(str(curva['codigo_curva']))} "
              f"mp:date={quoteattr(curva['data_liberacao'])}>"]
    pilares_ordenados = sorted(pilares, key=lambda p: _numero(p.get(campo_prazo), campo_prazo))
    for p in pilares_ordenados:
        if p.get(campo_prazo) in (None, "") or p.get(campo_taxa) in (None, ""):
            raise ErroTransformacao(f"pilar sem '{campo_prazo}' ou '{campo_taxa}': {p}")
        dias = int(_numero(p[campo_prazo], campo_prazo))
        atributos = f"mp:label=\"{dias}D\" mp:days=\"{dias}\" mp:rate={quoteattr(str(p[campo_taxa]))}"
        if usar_fator:
            atributos += f" mp:factor=\"{calcular_fator(p.get(campo_pu))}\""
        linhas.append(f"  <mp:pillar {atributos}/>")
    linhas.append("</mp:curve>")
    return "\n".join(linhas)


def gerar_xml(template, curva, pilares):
    if template.count(PALAVRA_CHAVE) != 1:
        raise ErroTransformacao("o template deve ter exatamente uma palavra-chave 'dadoslambda'")
    conteudo = template.replace(PALAVRA_CHAVE, montar_bloco_curva(curva, pilares))
    try:
        xml.dom.minidom.parseString(conteudo.encode("utf-8"))   # garante XML bem-formado
    except Exception as e:
        raise ErroTransformacao(f"XML gerado é inválido: {e}")
    return conteudo
```

#### `entrega_service.py`

Gera o nome do arquivo no padrão do guia e grava de forma atômica, sem duplicar nem sobrescrever. Destino: `local`, `s3` (`boto3`) ou `efs`.

```python
import errno
import hashlib
import os
import re
import uuid
from datetime import datetime

import boto3

from logger_config import logger


class ErroEntrega(Exception):
    pass


def nome_arquivo(curva):
    """Guia, seção 8: {nomeCurva}_{dataExecucao}_{versao}_{timestampUTC}.xml.
    O timestamp vem da data_inclusao do evento, então reprocessar gera o MESMO nome (não duplica)."""
    versao = os.getenv("TEMPLATE_VERSAO", "v1")
    inclusao = datetime.fromisoformat(curva["data_inclusao"].replace("Z", "+00:00"))
    carimbo = inclusao.strftime("%Y%m%dT%H%M%SZ")
    nome = f"{curva['nome_curva']}_{curva['data_liberacao']}_{versao}_{carimbo}.xml"
    if not re.fullmatch(r"[A-Za-z0-9_.\-]+", nome):
        raise ErroEntrega(f"nome de arquivo inválido: {nome}")
    return nome


def entregar(nome, conteudo):
    """DESTINO = local | s3 | efs. O destino final (EFS do guia ou API on-premises como em preços) está em definição."""
    destino = os.getenv("DESTINO", "local").lower()
    dados = conteudo.encode("utf-8")
    hash_arquivo = hashlib.sha256(dados).hexdigest()

    if destino == "s3":
        bucket = os.getenv("SAIDA_BUCKET")
        chave = os.getenv("SAIDA_PREFIX", "curvas_xml/") + nome
        boto3.client("s3", region_name=os.getenv("AWS_REGION_NAME", "sa-east-1")).put_object(
            Bucket=bucket, Key=chave, Body=dados, ContentType="application/xml")
        logger.info("XML gravado no S3.", bucket=bucket, chave=chave)
        return f"s3://{bucket}/{chave}", len(dados), hash_arquivo

    pasta = os.getenv("SAIDA_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "saida")
    if destino == "local":
        os.makedirs(pasta, exist_ok=True)
    return _gravar_atomico(pasta, nome, dados, hash_arquivo)


def _gravar_atomico(pasta, nome, dados, hash_arquivo):
    """Guia, seção 8: grava em arquivo temporário e só depois publica o nome final.
    Nunca sobrescreve: se o arquivo já existe igual, é reprocessamento (ok); se é diferente, é erro."""
    final = os.path.join(pasta, nome)
    temporario = os.path.join(pasta, f".{nome}.{uuid.uuid4().hex}.tmp")
    try:
        if os.path.exists(final):
            return _ja_existe(final, len(dados), hash_arquivo)
        with open(temporario, "xb") as f:
            f.write(dados)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(temporario, final)            # atômico e falha se o final já existir
        except FileExistsError:
            return _ja_existe(final, len(dados), hash_arquivo)
        except OSError as e:
            if e.errno not in (errno.EPERM, errno.ENOTSUP, errno.EXDEV):
                raise
            if os.path.exists(final):
                return _ja_existe(final, len(dados), hash_arquivo)
            os.replace(temporario, final)          # sistema sem hard link (ex.: Windows local)
        return final, len(dados), hash_arquivo
    except ErroEntrega:
        raise
    except OSError as e:
        raise ErroEntrega(f"falha ao gravar em {pasta}: {e.strerror or e}")
    finally:
        if os.path.exists(temporario):
            os.remove(temporario)


def _ja_existe(final, tamanho, hash_arquivo):
    with open(final, "rb") as f:
        if hashlib.sha256(f.read()).hexdigest() == hash_arquivo:
            return final, tamanho, hash_arquivo
    raise ErroEntrega(f"arquivo já existe com conteúdo diferente: {os.path.basename(final)}")
```

#### `curva_template.xml`

Template XML com o mesmo envelope do template de preço da documentação. O bloco da curva é provisório.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!-- Envelope igual ao template de preço (FGEQ) da documentação. O bloco interno é PROVISÓRIO até o contrato XML/XSD de curva do Murex. -->
<GuiRoot xmlns:a="http://schemas.datacontract.org/2004/07/itau.im.entidade"
    xmlns:mp="mx.MarketParameters" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xc="XmlCache"
    xmlns:NS1="http://schemas.xmlsoap.org/soap/envelope/">
    <xc:XmlCache xc:action="Update">
        <xc:XmlCacheArea xc:value="MarketParameters">
            <mp:nickName xc:value="TR_EODINST">dadoslambda</mp:nickName>
        </xc:XmlCacheArea>
    </xc:XmlCache>
</GuiRoot>
```

#### `teste_local.py`

Teste local sem AWS: gera os XMLs na pasta `saida`, com substitutos se as bibliotecas não estiverem instaladas.

```python
"""
Teste local da gravardadoscurvas, sem AWS.

  python teste_local.py                                   -> usa mensagem_teste.json
  python teste_local.py fator                             -> idem, calculando também Fator = 100000 / PU
  python teste_local.py ..\testeaws\mensagens_sqs.json    -> usa as mensagens geradas pela captura (fluxo completo)

Gera os XMLs na pasta 'saida'. A 3ª mensagem de mensagem_teste.json é inválida de propósito (sem pilares).
Rodar duas vezes mostra a idempotência: o mesmo arquivo não é duplicado nem sobrescrito.

Instale as bibliotecas com:  pip install -r requirements.txt
Se o pip estiver bloqueado, o teste roda mesmo assim usando substitutos locais.
"""
import json
import os
import sys

PASTA = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PASTA)
os.environ.setdefault("LOG_JSON", "false")
os.environ.setdefault("DESTINO", "local")

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

args = sys.argv[1:]
if "fator" in [a.lower() for a in args]:
    os.environ["CALCULAR_FATOR"] = "true"
arquivos = [a for a in args if a.lower().endswith(".json")]
ARQUIVO_MENSAGENS = arquivos[0] if arquivos else os.path.join(PASTA, "mensagem_teste.json")

import lambda_function  # noqa: E402

print(f"Mensagens de entrada: {ARQUIVO_MENSAGENS}")
with open(ARQUIVO_MENSAGENS, encoding="utf-8") as f:
    mensagens = json.load(f)

evento = {"Records": [{"messageId": f"msg-{i}", "body": json.dumps(m)} for i, m in enumerate(mensagens, 1)]}
resultado = lambda_function.lambda_handler(evento, None)

print("\n=== RESULTADO ===")
print(json.dumps(resultado, indent=2, ensure_ascii=False))

for r in resultado["resultados"]:
    if r.get("status") == "ENTREGUE":
        with open(r["arquivo"], encoding="utf-8") as f:
            print(f"\n===== {os.path.basename(r['arquivo'])} =====\n{f.read()}")
```

#### `mensagem_teste.json`

3 mensagens de teste: BRL, USD e uma inválida (sem pilares).

```json
[
  {
    "correlation_id": "teste-0001",
    "curva": {"codigo_curva": "2361", "nome_curva": "BRL_KLm0_RM01", "tipo_curva": "BASE",
              "data_liberacao": "20261112", "data_inclusao": "2026-11-12T18:05:00Z"},
    "pilares": [
      {"prazo_dias": 180, "taxa": "10.9300", "pu": "95100.10"},
      {"prazo_dias": 1, "taxa": "10.6500", "pu": "99970.80"},
      {"prazo_dias": 90, "taxa": "10.8100", "pu": "97450.25"}
    ]
  },
  {
    "correlation_id": "teste-0002",
    "curva": {"codigo_curva": "2305", "nome_curva": "USD_KLm0_RM01", "tipo_curva": "BASE",
              "data_liberacao": "20261112", "data_inclusao": "2026-11-12T18:06:00Z"},
    "pilares": [
      {"prazo_dias": 30, "taxa": "4.3100", "pu": "99640.00"},
      {"prazo_dias": 360, "taxa": "4.0500", "pu": "96100.00"}
    ]
  },
  {
    "correlation_id": "teste-0003",
    "curva": {"codigo_curva": "9999", "nome_curva": "CURVA_SEM_PILARES", "tipo_curva": "BASE",
              "data_liberacao": "20261112", "data_inclusao": "2026-11-12T18:07:00Z"},
    "pilares": []
  }
]
```
