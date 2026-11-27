# Lambdas de Curvas — IM8 → Murex Uruguai

Duas Lambdas no padrão das de preços (`obterdadospreco` / `enviardadospreco`), com escopo de 3 curvas: CUD (6464), CUI (6453) e ITLUP (6475).

```
IM8 (Kafka) ─Connector─▶ obterdadoscurva ─SQS de curvas─▶ gravardadoscurvas ─▶ XML do Murex
```

- [Parte 1 — Obter Dados Curvas](#parte-1--obter-dados-curvas-obterdadoscurva)
- [Parte 2 — Gravar Dados Murex Uruguai — Curvas](#parte-2--gravar-dados-murex-uruguai--curvas-gravardadoscurvas)

**Teste do fluxo completo (local):**
1. Na pasta `obterdadoscurva`: `python teste_local.py`
2. Na pasta `gravardadoscurvas`: `python teste_local.py ..\obterdadoscurva\mensagens_sqs.json`

---

## Parte 1 — Obter Dados Curvas (`obterdadoscurva`)

Lambda que captura as curvas do IM8 e manda para a Lambda de gravar dados. Segue o *Guia de Captura de Curvas IM8 v1.0* e o **padrão da `obterdadospreco`**: `requests` (com retry), `loguru` (logs em JSON), `boto3` (Secrets Manager e SQS), credencial no `InfraSecretsMX3` e token no STS.

**Escopo:** só 3 curvas (definido com o Alex):

| Código | Curva |
|---|---|
| 6464 | `CUD_KLm0_RM01` |
| 6453 | `CUI_KLm0_RM01` |
| 6475 | `ITLUP_KLm0_RM01` |

As demais curvas que chegarem do tópico são marcadas como `IGNORADA` e não geram chamada à API.

**Fluxo:**
1. Evento do Connector (Kafka).
2. Valida o evento e filtra as 3 curvas.
3. Credenciais (`InfraSecretsMX3`) → token (STS).
4. Tipo da curva pelo `cadastrocurvas` (quando o evento não traz).
5. Pontos pelo `pontoscurvas` (BASE/COMPOSTA) ou `pontotransferencias` (TRANSFER).
6. Descarta os pontos interpolados e fica só com os pilares.
7. Envia curva + pilares para a SQS de curvas.

Se nenhuma curva do lote estiver no escopo, a Lambda nem busca credencial ou token.

**Instalar:** `pip install -r requirements.txt` (o `boto3` já vem no runtime da Lambda).

**Teste local:**
- `python teste_local.py`: simulado, sem credencial. Gera o `mensagens_sqs.json` para a Lambda de gravar.
- `python teste_local.py rede`: só testa o acesso ao STS e à API de curvas.

Se o pip estiver bloqueado, o teste usa substitutos locais.

**Pendente:**
- Credencial (clientID) de curvas no IM8.
- URL da SQS de curvas.
- Confirmar o nome real do campo de interpolação na resposta da API.

### Variáveis de ambiente

| Chave | Valor (dev) |
|---|---|
| `AWS_REGION_NAME` | `sa-east-1` |
| `SECRET_MANAGER_NAME` | `InfraSecretsMX3` |
| `AWS_LAMBDA_CLIENT_ID` / `AWS_LAMBDA_SECRET_ID` | nomes das chaves do clientID de **curvas** dentro do segredo (pendente) |
| `URLSts` | `https://des-sts-int.mbi.cloud.ihf` |
| `API_BASE_URL` | `https://curvas-v1-distribuiprecoscorporativos.api-sp.dev.aws.cloud.ihf/curvas/v1/` |
| `X_APIGW_API_ID` | vazio (a API de curvas não usa) |
| `CA_CERT_PATH` | `/opt/certs/ca_bundle.crt` (layer `cloudsec-itau-ca-bundle-lambda-layer`) |
| `CURVAS_PERMITIDAS` | opcional; padrão `6464,6453,6475` |
| `CAMPO_INTERPOLADO` | padrão `indicador_interpolado` (confirmar com a resposta real) |
| `FILA_CURVAS_URL` | URL da SQS de curvas (vazio = só registra no log, não envia) |
| `LOG_JSON` | `true` na AWS (padrão); `false` deixa o log legível no teste local |

### Arquivos

#### `requirements.txt`

```text
# Lambda de captura de curvas (obterdadoscurvas) — mesmo padrão da obterdadospreco
requests>=2.31
loguru>=0.7
# boto3 já vem no runtime Python da AWS Lambda (não precisa empacotar).
# Para testar fora da AWS com chamadas reais ao Secrets Manager / SQS, instale também:
# boto3>=1.28
```

#### `lambda_function.py`

```python
"""
obterdadoscurvas — Lambda de captura de curvas do IM8 (mesmo padrão da obterdadospreco).

Fluxo: evento do Connector -> valida a curva -> filtra as curvas do escopo (CUD, CUI, ITLUP) -> credenciais (InfraSecretsMX3) -> token (STS)
       -> tipo pelo cadastrocurvas (se faltar) -> pontos na API de curvas -> filtra os pilares
       -> envia curva + pilares para a SQS de curvas (Lambda de gravar dados).
"""
import uuid

from credentials_service import obtain_client_credentials
from curva_event_model import CurvaEventModel, EventoInvalido
from curvas_escopo import esta_no_escopo
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

    # 1) Valida e filtra antes de qualquer chamada: o tópico traz todas as curvas do IM8,
    #    mas só as do escopo (CUD, CUI, ITLUP) seguem. As demais não geram chamada à API.
    resultados, curvas = [], []
    for elemento in registros:
        try:
            curva = CurvaEventModel.from_event(elemento["payload"]["value"])
        except (EventoInvalido, KeyError, TypeError) as e:
            logger.error("Evento de curva inválido, descartado.", erro=str(e))
            resultados.append({"status": "INVALIDO", "erro": str(e)})
            continue
        if not esta_no_escopo(curva.codigo_curva):
            logger.info("Curva fora do escopo, ignorada.", codigo_curva=curva.codigo_curva, nome_curva=curva.nome_curva)
            resultados.append({"codigo_curva": curva.codigo_curva, "nome_curva": curva.nome_curva, "status": "IGNORADA"})
            continue
        curvas.append(curva)

    if not curvas:
        logger.info("Nenhuma curva do escopo no lote.", total=len(resultados))
        return {"correlation_id": correlation_id, "resultados": resultados}

    # 2) Credenciais e token só quando há curva do escopo para buscar.
    credentials_model = obtain_client_credentials()
    sts_token = StsService().obtain_sts_token(credentials_model)
    im8_service = Im8CurvasService(correlation_id, credentials_model.client_id)

    for curva in curvas:
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

#### `curvas_escopo.py`

```python
import os

# Curvas do escopo Murex Uruguai (definidas com o Alex em 27/11/2026). Pode ser sobrescrito pela variável CURVAS_PERMITIDAS.
CURVAS_PADRAO = {
    "6464": "CUD_KLm0_RM01",
    "6453": "CUI_KLm0_RM01",
    "6475": "ITLUP_KLm0_RM01",
}


def curvas_permitidas():
    valor = os.getenv("CURVAS_PERMITIDAS")
    if valor:
        return {c.strip() for c in valor.split(",") if c.strip()}
    return set(CURVAS_PADRAO)


def esta_no_escopo(codigo_curva):
    return str(codigo_curva).strip() in curvas_permitidas()
```

#### `logger_config.py`

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

```python
class HttpRequestError(Exception):
    def __init__(self, message, status_code, url_called):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.url_called = url_called
```

#### `credentials_model.py`

```python
from dataclasses import dataclass


@dataclass
class CredentialsModel:
    client_id: str
    client_secret: str
```

#### `credentials_service.py`

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

```json
[
  {
    "payload": {
      "key": null,
      "value": {
        "codigo_curva": "6475",
        "nome_curva": "ITLUP_KLm0_RM01",
        "nome_tipo_curva": "BASE",
        "data_liberacao": "20261126",
        "data_inclusao": "2026-11-26T18:05:00Z"
      }
    }
  }
]
```

#### `evento_teste_3_curvas.json`

```json
[
  {
    "payload": {
      "key": null,
      "value": {
        "codigo_curva": "6464",
        "nome_curva": "CUD_KLm0_RM01",
        "nome_tipo_curva": "BASE",
        "data_liberacao": "20261126",
        "data_inclusao": "2026-11-26T18:05:00Z"
      }
    }
  },
  {
    "payload": {
      "key": null,
      "value": {
        "codigo_curva": "6453",
        "nome_curva": "CUI_KLm0_RM01",
        "nome_tipo_curva": "BASE",
        "data_liberacao": "20261126",
        "data_inclusao": "2026-11-26T18:05:00Z"
      }
    }
  },
  {
    "payload": {
      "key": null,
      "value": {
        "codigo_curva": "6475",
        "nome_curva": "ITLUP_KLm0_RM01",
        "nome_tipo_curva": "BASE",
        "data_liberacao": "20261126",
        "data_inclusao": "2026-11-26T18:05:00Z"
      }
    }
  },
  {
    "payload": {
      "key": null,
      "value": {
        "codigo_curva": "2361",
        "nome_curva": "BRL_KLm0_RM01",
        "nome_tipo_curva": "BASE",
        "data_liberacao": "20261126",
        "data_inclusao": "2026-11-26T18:05:00Z"
      }
    }
  }
]
```

---

## Parte 2 — Gravar Dados Murex Uruguai — Curvas (`gravardadoscurvas`)

Lambda que transforma as curvas capturadas no XML de curvas do Murex, **no formato enviado pelo Alex**. Segue o **padrão da `enviardadospreco`**: template com a palavra-chave `dadoslambda` (no S3, pasta `templates-xml-murex/`), `loguru` (logs em JSON) e `boto3`.

**Fluxo:**
1. Mensagem da SQS, enviada pela captura.
2. Valida a mensagem.
3. Busca a configuração Murex da curva (`curvas_murex.json`).
4. Pega o template XML.
5. Troca o `dadoslambda` pelo bloco `currency / type / generator / market / maturity` (`mp:ask` e `mp:bid`).
6. Valida o XML.
7. Grava de forma atômica no destino, sem sobrescrever.

Erros voltam em `batchItemFailures`: só a mensagem que falhou volta para a fila.

**Configuração das 3 curvas (`curvas_murex.json`):**

| Código | Curva | Status |
|---|---|---|
| 6475 | `ITLUP_KLm0_RM01` | completa (UYU / Dep.Gen. / UYU ITLUP CURVE), gera o XML |
| 6464 | `CUD_KLm0_RM01` | **pendente**: falta `currency`, `type` e `generator` (Alex) |
| 6453 | `CUI_KLm0_RM01` | **pendente**: falta `currency`, `type` e `generator` (Alex) |

Enquanto uma curva estiver pendente, ela dá o erro "configuração Murex pendente" e não gera arquivo. Quando o Alex passar os valores, é só preencher o JSON, sem mudar o código.

**Rótulo do vencimento:** usa o campo `vertice`, se a API mandar. Se não mandar, converte o prazo em dias:

| Prazo em dias | Rótulo |
|---|---|
| até 1 | `O/N` |
| múltiplo de 360 ou de 365 | anos (`1Y` … `8Y`) |
| múltiplo de 30 | meses (`1M`, `2M`, `3M`, `6M`) |
| outros | dias (`45D`) |

A regra é provisória: confirmar com o Alex.

**Instalar:** `pip install -r requirements.txt` (o `boto3` já vem no runtime da Lambda).

**Teste local:**
- `python teste_local.py`: usa o `mensagem_teste.json`.
  - 1ª mensagem: ITLUP, gera o XML igual ao do Alex.
  - 2ª mensagem: CUD, erro esperado (configuração pendente).
  - 3ª mensagem: curva sem pilares, erro proposital.
- `python teste_local.py ..\obterdadoscurva\mensagens_sqs.json`: fluxo completo com as mensagens geradas pela captura.

Os XMLs saem na pasta `saida`. Se o pip estiver bloqueado, o teste usa substitutos locais.

**Pendente:**
- `currency`, `type` e `generator` da CUD e da CUI.
- Regra dias → rótulo.
- `TODAY` × data real no reprocessamento.
- Destino final (EFS ou API on-premises).
- SQS de curvas.

### Variáveis de ambiente

| Chave | Valor |
|---|---|
| `DESTINO` | `local` (teste), `s3` ou `efs` |
| `SAIDA_DIR` | pasta de saída para `local`/`efs` (padrão: `saida`) |
| `SAIDA_BUCKET` / `SAIDA_PREFIX` | bucket e prefixo quando `DESTINO=s3` (prefixo padrão `curvas_xml/`) |
| `TEMPLATES_BUCKET` | bucket dos templates (vazio = usa o `curva_template.xml` local) |
| `TEMPLATES_PREFIX` | padrão `templates-xml-murex/` |
| `TEMPLATE_CURVA` | padrão `curva_template.xml` |
| `TEMPLATE_VERSAO` | versão no nome do arquivo (padrão `v1`) |
| `CURVAS_MUREX_CONFIG` | opcional: o JSON de configuração das curvas em texto (vazio = usa o `curvas_murex.json`) |
| `CAMPO_PRAZO` / `CAMPO_TAXA` / `CAMPO_VERTICE` | nomes dos campos dos pilares (padrão `prazo_dias`, `taxa`, `vertice`) |
| `AWS_REGION_NAME` | `sa-east-1` |
| `LOG_JSON` | `true` na AWS (padrão); `false` deixa o log legível no teste local |

### Arquivos

#### `requirements.txt`

```text
# Lambda de gravar dados de curvas (gravardadoscurvas) — mesmo padrão da enviardadospreco
loguru>=0.7
# boto3 já vem no runtime Python da AWS Lambda (não precisa empacotar).
# Para testar fora da AWS lendo templates / gravando no S3, instale também:
# boto3>=1.28
```

#### `lambda_function.py`

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

#### `mensagem_model.py`

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

```python
"""Monta o XML de curva no formato do Murex (exemplo enviado pelo Alex):

<rtsh:sheet>                                  <- fica no template (curva_template.xml)
  <rtsh:currency xc:value="UYU">              <- daqui para baixo é montado aqui e entra no lugar de 'dadoslambda'
    <rtsh:type xc:value="Dep.Gen.">
      <rtsh:generator xc:value="UYU ITLUP CURVE">
        <rtsh:market>
          <rtsh:maturity xc:type="Fields" xc:value="O/N">
            <mp:ask xc:type="Field" xc:keyFormat="N">6.0</mp:ask>
            <mp:bid xc:type="Field" xc:keyFormat="N">6.0</mp:bid>
          </rtsh:maturity>
          ...
"""
import json
import os
import xml.dom.minidom
from decimal import Decimal, InvalidOperation
from xml.sax.saxutils import escape, quoteattr

from template_service import PALAVRA_CHAVE


class ErroTransformacao(Exception):
    pass


_config_cache = {}


def _numero(valor, nome):
    try:
        return Decimal(str(valor).strip().replace(",", "."))
    except (InvalidOperation, ValueError):
        raise ErroTransformacao(f"valor não numérico em '{nome}': {valor!r}")


def obter_config_murex(codigo_curva):
    """currency / type / generator do Murex para a curva (arquivo curvas_murex.json)."""
    caminho = os.getenv("CURVAS_MUREX_CONFIG") or os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                               "curvas_murex.json")
    if caminho not in _config_cache:
        with open(caminho, encoding="utf-8") as f:
            _config_cache[caminho] = json.load(f)
    config = _config_cache[caminho].get(str(codigo_curva))
    if not config:
        raise ErroTransformacao(f"curva {codigo_curva} sem configuração Murex (currency/type/generator) em curvas_murex.json")
    pendentes = [c for c in ("currency", "type", "generator") if not config.get(c)]
    if pendentes:
        raise ErroTransformacao(f"configuração Murex pendente para a curva {codigo_curva}: {', '.join(pendentes)}")
    return config


def rotulo_vencimento(pilar, campo_prazo, campo_vertice):
    """Rótulo do vencimento no padrão do Murex: O/N, 1M, 2M, 3M, 6M, 1Y, 2Y...
    Se o IM8 já mandar o rótulo (campo CAMPO_VERTICE), usa ele. Senão, converte os dias.
    PROVISÓRIO: a regra dias -> rótulo precisa ser confirmada com o Alex / resposta real do IM8."""
    rotulo = str(pilar.get(campo_vertice) or "").strip()
    if rotulo:
        return rotulo
    dias = int(_numero(pilar.get(campo_prazo), campo_prazo))
    if dias <= 1:
        return "O/N"
    if dias % 360 == 0:
        return f"{dias // 360}Y"
    if dias % 365 == 0:
        return f"{dias // 365}Y"
    if dias % 30 == 0:
        return f"{dias // 30}M"
    return f"{dias}D"


def montar_bloco_curva(curva, pilares):
    campo_prazo = os.getenv("CAMPO_PRAZO", "prazo_dias")
    campo_taxa = os.getenv("CAMPO_TAXA", "taxa")
    campo_vertice = os.getenv("CAMPO_VERTICE", "vertice")
    config = obter_config_murex(curva["codigo_curva"])

    def chave_ordem(p):
        return _numero(p.get(campo_prazo), campo_prazo) if p.get(campo_prazo) not in (None, "") else Decimal(0)

    linhas = [f"<rtsh:currency xc:value={quoteattr(config['currency'])}>",
              f"<rtsh:type xc:value={quoteattr(config['type'])}>",
              f"<rtsh:generator xc:value={quoteattr(config['generator'])}>",
              "<rtsh:market>"]
    vistos = set()
    for p in sorted(pilares, key=chave_ordem):
        if p.get(campo_taxa) in (None, ""):
            raise ErroTransformacao(f"pilar sem '{campo_taxa}': {p}")
        rotulo = rotulo_vencimento(p, campo_prazo, campo_vertice)
        if rotulo in vistos:
            raise ErroTransformacao(f"vencimento repetido na curva {curva['codigo_curva']}: {rotulo}")
        vistos.add(rotulo)
        taxa = str(p[campo_taxa]).strip()
        _numero(taxa, campo_taxa)                       # garante que é número
        valor = escape(taxa)
        linhas += [f"<rtsh:maturity xc:type=\"Fields\" xc:value={quoteattr(rotulo)}>",
                   f"<mp:ask xc:type=\"Field\" xc:keyFormat=\"N\">{valor}</mp:ask>",
                   f"<mp:bid xc:type=\"Field\" xc:keyFormat=\"N\">{valor}</mp:bid>",
                   "</rtsh:maturity>"]
    linhas += ["</rtsh:market>", "</rtsh:generator>", "</rtsh:type>", "</rtsh:currency>"]
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

#### `curvas_murex.json`

```json
{
  "_comentario": "Configuração Murex por código da curva (IM8): currency/type/generator vão no XML (rtsh:currency, rtsh:type, rtsh:generator). Escopo: CUD, CUI e ITLUP. Só a ITLUP veio no exemplo do Alex; preencher CUD e CUI com ele (null = pendente: a curva dá erro até ser preenchida).",
  "6464": {
    "nome_curva": "CUD_KLm0_RM01",
    "currency": null,
    "type": null,
    "generator": null
  },
  "6453": {
    "nome_curva": "CUI_KLm0_RM01",
    "currency": null,
    "type": null,
    "generator": null
  },
  "6475": {
    "nome_curva": "ITLUP_KLm0_RM01",
    "currency": "UYU",
    "type": "Dep.Gen.",
    "generator": "UYU ITLUP CURVE"
  }
}
```

#### `curva_template.xml`

```xml
<xc:XmlCache xmlns:xc="XmlCache">
<xc:XmlCacheArea xc:value="MarketParameters">
<mp:nickName xmlns:mp="mx.MarketParameters" xc:value="MXDEFAULT">
<mp:date xc:value="TODAY">
<rt:rate xmlns:rt="mx.MarketParameters.Rates">
<rtsh:sheet xmlns:rtsh="mx.MarketParameters.Rates.Sheet">
dadoslambda
</rtsh:sheet>
</rt:rate>
</mp:date>
</mp:nickName>
</xc:XmlCacheArea>
</xc:XmlCache>
```

#### `entrega_service.py`

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

#### `logger_config.py`

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

#### `teste_local.py`

```python
r"""
Teste local da gravardadoscurvas, sem AWS.

  python teste_local.py                                   -> usa mensagem_teste.json
  python teste_local.py ..\obterdadoscurva\mensagens_sqs.json    -> usa as mensagens geradas pela captura (fluxo completo)

Gera os XMLs na pasta 'saida'. mensagem_teste.json: 1ª = curva ITLUP (exemplo do Alex), 2ª = CUD, configuração Murex ainda pendente (erro esperado),
3ª = sem pilares (erro proposital).
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

```json
[
  {
    "correlation_id": "teste-0001",
    "curva": {
      "codigo_curva": "6475",
      "nome_curva": "ITLUP_KLm0_RM01",
      "tipo_curva": "BASE",
      "data_liberacao": "20261126",
      "data_inclusao": "2026-11-26T18:05:00Z"
    },
    "pilares": [
      {
        "prazo_dias": 2880,
        "taxa": "7.73"
      },
      {
        "prazo_dias": 2520,
        "taxa": "7.55"
      },
      {
        "prazo_dias": 2160,
        "taxa": "7.35"
      },
      {
        "prazo_dias": 1800,
        "taxa": "7.15"
      },
      {
        "prazo_dias": 1440,
        "taxa": "6.9"
      },
      {
        "prazo_dias": 1080,
        "taxa": "6.7"
      },
      {
        "prazo_dias": 720,
        "taxa": "6.5"
      },
      {
        "prazo_dias": 360,
        "taxa": "6.25"
      },
      {
        "prazo_dias": 180,
        "taxa": "6.15"
      },
      {
        "prazo_dias": 90,
        "taxa": "6.13"
      },
      {
        "prazo_dias": 60,
        "taxa": "6.12"
      },
      {
        "prazo_dias": 30,
        "taxa": "6.1"
      },
      {
        "prazo_dias": 1,
        "taxa": "6.0"
      }
    ]
  },
  {
    "correlation_id": "teste-0002",
    "curva": {
      "codigo_curva": "6464",
      "nome_curva": "CUD_KLm0_RM01",
      "tipo_curva": "BASE",
      "data_liberacao": "20261126",
      "data_inclusao": "2026-11-26T18:06:00Z"
    },
    "pilares": [
      {
        "prazo_dias": 30,
        "taxa": "10.65"
      }
    ]
  },
  {
    "correlation_id": "teste-0003",
    "curva": {
      "codigo_curva": "6475",
      "nome_curva": "ITLUP_KLm0_RM01",
      "tipo_curva": "BASE",
      "data_liberacao": "20261126",
      "data_inclusao": "2026-11-26T18:07:00Z"
    },
    "pilares": []
  }
]
```

### Exemplo de saída (ITLUP, gerado pelo `teste_local.py`)

Arquivo `ITLUP_KLm0_RM01_20261126_v1_20261126T180500Z.xml`:

```xml
<xc:XmlCache xmlns:xc="XmlCache">
<xc:XmlCacheArea xc:value="MarketParameters">
<mp:nickName xmlns:mp="mx.MarketParameters" xc:value="MXDEFAULT">
<mp:date xc:value="TODAY">
<rt:rate xmlns:rt="mx.MarketParameters.Rates">
<rtsh:sheet xmlns:rtsh="mx.MarketParameters.Rates.Sheet">
<rtsh:currency xc:value="UYU">
<rtsh:type xc:value="Dep.Gen.">
<rtsh:generator xc:value="UYU ITLUP CURVE">
<rtsh:market>
<rtsh:maturity xc:type="Fields" xc:value="O/N">
<mp:ask xc:type="Field" xc:keyFormat="N">6.0</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">6.0</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="1M">
<mp:ask xc:type="Field" xc:keyFormat="N">6.1</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">6.1</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="2M">
<mp:ask xc:type="Field" xc:keyFormat="N">6.12</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">6.12</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="3M">
<mp:ask xc:type="Field" xc:keyFormat="N">6.13</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">6.13</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="6M">
<mp:ask xc:type="Field" xc:keyFormat="N">6.15</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">6.15</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="1Y">
<mp:ask xc:type="Field" xc:keyFormat="N">6.25</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">6.25</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="2Y">
<mp:ask xc:type="Field" xc:keyFormat="N">6.5</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">6.5</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="3Y">
<mp:ask xc:type="Field" xc:keyFormat="N">6.7</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">6.7</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="4Y">
<mp:ask xc:type="Field" xc:keyFormat="N">6.9</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">6.9</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="5Y">
<mp:ask xc:type="Field" xc:keyFormat="N">7.15</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">7.15</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="6Y">
<mp:ask xc:type="Field" xc:keyFormat="N">7.35</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">7.35</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="7Y">
<mp:ask xc:type="Field" xc:keyFormat="N">7.55</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">7.55</mp:bid>
</rtsh:maturity>
<rtsh:maturity xc:type="Fields" xc:value="8Y">
<mp:ask xc:type="Field" xc:keyFormat="N">7.73</mp:ask>
<mp:bid xc:type="Field" xc:keyFormat="N">7.73</mp:bid>
</rtsh:maturity>
</rtsh:market>
</rtsh:generator>
</rtsh:type>
</rtsh:currency>
</rtsh:sheet>
</rt:rate>
</mp:date>
</mp:nickName>
</xc:XmlCacheArea>
</xc:XmlCache>
```
