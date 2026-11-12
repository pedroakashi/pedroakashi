# Captura de Curvas IM8 → Murex Uruguai

Primeira versão das Lambdas de curvas, seguindo o guia de captura de curvas do IM8 e o padrão da `obterdadospreco` / `enviardadospreco`.

> **Provisório:** o formato da resposta da API de curvas do IM8, o nome do campo de interpolado e o bloco da curva no XML ainda precisam ser confirmados com dados reais.

## obterdadoscurvas

Lambda de captura: recebe o evento de curva do Kafka (Connector Lambda Sink), valida, obtém o token no STS, chama a API de curvas do IM8 e filtra só os pilares (pontos não interpolados).

### `curva_event_model.py`

Modelo e validação do evento de curva.

```python
import re
from dataclasses import dataclass
from datetime import datetime

TIPOS_VALIDOS = ("BASE", "COMPOSTA", "TRANSFER")


class EventoInvalido(Exception):
    pass


@dataclass
class CurvaEventModel:
    """Evento do tópico de curvas (guia de curvas, seção 5)."""
    codigo_curva: str
    nome_curva: str
    tipo_curva: str
    data_liberacao: str      # yyyymmdd: data usada para buscar os pontos
    data_inclusao: str

    @classmethod
    def from_event(cls, value: dict) -> "CurvaEventModel":
        for campo in ("codigo_curva", "nome_curva", "nome_tipo_curva", "data_liberacao", "data_inclusao"):
            if not str(value.get(campo) or "").strip():
                raise EventoInvalido(f"campo obrigatório ausente: {campo}")

        tipo = str(value["nome_tipo_curva"]).strip().upper()
        if tipo not in TIPOS_VALIDOS:
            raise EventoInvalido(f"nome_tipo_curva desconhecido: {tipo}")

        return cls(codigo_curva=str(value["codigo_curva"]).strip(),
                   nome_curva=str(value["nome_curva"]).strip(),
                   tipo_curva=tipo,
                   data_liberacao=_para_yyyymmdd(str(value["data_liberacao"]).strip()),
                   data_inclusao=str(value["data_inclusao"]).strip())


def _para_yyyymmdd(texto: str) -> str:
    if re.fullmatch(r"\d{8}", texto):
        return texto
    if re.match(r"\d{4}-\d{2}-\d{2}", texto):
        return datetime.strptime(texto[:10], "%Y-%m-%d").strftime("%Y%m%d")
    raise EventoInvalido(f"data_liberacao inválida: {texto}")
```

### `credentials_service.py`

Busca client_id e client_secret no Secrets Manager (padrão InfraSecretsMX3).

```python
import json
import os
from dataclasses import dataclass
import boto3
from botocore.exceptions import ClientError
from logger_config import logger


@dataclass
class CredentialsModel:
    client_id: str
    client_secret: str


def obtain_client_credentials():
    # Mesmo padrão da obterdadospreco: segredo compartilhado (SECRET_MANAGER_NAME) com uma chave por lambda.
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
    except Exception as e:
        logger.exception("Ocorreu um erro inesperado durante a recuperação das credenciais.", secret_name=secret_name)
        raise e
```

### `sts_service.py`

Obtém o token no STS.

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
        request_path = self.url_base + "/api/oauth/token"
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

            logger.exception("Falha no sts request. Não foi possível obter o token.",
                             status_code_error=response.status_code, error_message=response.reason,
                             url_called=response.url)
            raise HttpRequestError(message=response.reason, status_code=response.status_code,
                                   url_called=response.url)
        except Exception as e:
            logger.exception("Ocorreu um erro inesperado durante a recuperação do token jwt do sts.")
            raise e
```

### `im8_curvas_service.py`

Chama a API de curvas do IM8 (pontoscurvas ou pontotransferencias, conforme o tipo).

```python
import os
from urllib.parse import quote
import requests
from requests.adapters import HTTPAdapter, Retry
from http_request_error import HttpRequestError
from logger_config import logger


class Im8CurvasService:
    """Chama as APIs de Curvas do IM8 (guia de curvas, seção 6), no mesmo padrão do im8_service.py.

    A URL é montada pela configuração (URL_IM8_CURVAS); o campo `url` do evento nunca é chamado.
    """

    def __init__(self, correlation_id, client_id):
        self.base_url = os.getenv("URL_IM8_CURVAS").rstrip("/")
        self.x_itau_apikey = client_id
        self.x_itau_correlation_id = correlation_id
        self.x_apigw_api_id = os.getenv("X_APIGW_API_ID_CURVAS")
        self.certificate_path = os.getenv("CA_CERT_PATH")

    def obter_pontos(self, sts_token, curva):
        # TRANSFER usa pontotransferencias; BASE e COMPOSTA usam pontoscurvas (guia, 6.4)
        if curva.tipo_curva == "TRANSFER":
            url = f"{self.base_url}/curvas/v1/pontotransferencias"
            params = {"codigocurva": curva.codigo_curva, "dataexecucaocurva": curva.data_liberacao}
        else:
            url = f"{self.base_url}/curvas/v1/pontoscurvas/{quote(curva.nome_curva, safe='')}"
            params = {"data_execucao": curva.data_liberacao}
        return self._call_im8_api(sts_token, url, params)

    def _call_im8_api(self, sts_token, url, params):
        try:
            session = requests.Session()
            retries = Retry(total=6, backoff_factor=1, status_forcelist=[502, 503, 504],
                            allowed_methods={"GET"})
            session.mount("https://", HTTPAdapter(max_retries=retries))

            headers = {
                "x-itau-apikey": self.x_itau_apikey,
                "x-itau-correlationID": self.x_itau_correlation_id,
                "Authorization": "Bearer " + sts_token,
                "Accept-Encoding": "gzip",
            }
            if self.x_apigw_api_id:
                headers["x-apigw-api-id"] = self.x_apigw_api_id

            response = session.get(url=url, params=params, headers=headers,
                                   verify=self.certificate_path, timeout=15)

            if response.status_code == 200:
                return response.json()

            logger.error("Falha na chamada da api de curvas do IM8.", status_code_error=response.status_code,
                         error_message=response.reason, url_called=response.url)
            raise HttpRequestError(message=response.reason, status_code=response.status_code,
                                   url_called=response.url)
        except HttpRequestError:
            raise
        except Exception:
            logger.exception("Erro inesperado na chamada da api de curvas do IM8.", url=url)
            raise
```

### `pilares_service.py`

Filtra os pontos interpolados e mantém só os pilares.

```python
import os
from logger_config import logger

# Nome do campo que o IM8 usa para marcar ponto interpolado. PROVISÓRIO: confirmar com uma resposta real da API.
CAMPO_INTERPOLADO_PADRAO = "indicador_interpolado"


def filtrar_pilares(resposta_im8):
    """Mantém só os pilares da curva: pontos que o IM8 NÃO marcou como interpolados (reunião de 10/11)."""
    campo = os.getenv("CAMPO_INTERPOLADO", CAMPO_INTERPOLADO_PADRAO)
    pontos = resposta_im8.get("pontos", []) if isinstance(resposta_im8, dict) else resposta_im8

    pilares = [p for p in pontos if str(p.get(campo) or "").strip().upper() in ("", "N", "FALSE", "0")]
    logger.info("Pilares filtrados.", total_pontos=len(pontos), total_pilares=len(pilares), campo_interpolado=campo)
    return pilares
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

### `evento_teste.json`

Evento de exemplo no formato do Connector (payload.value).

```json
[
  {"payload": {"value": {
    "codigo_curva": "2361", "nome_curva": "BRL_KLm0_RM01", "nome_tipo_curva": "BASE",
    "data_liberacao": "20261109", "data_inclusao": "2026-11-09T18:05:00Z",
    "data_execucao_processo": "2026-11-09T18:00:00Z",
    "url": "https://curvas-v1-distribuiprecoscorporativos.api-sp.dev.aws.cloud.ihf/curvas/v1/pontoscurvas/BRL_KLm0_RM01"
  }}},
  {"payload": {"value": {
    "codigo_curva": "4540", "nome_curva": "USDBZG_Spm0_RM01", "nome_tipo_curva": "TRANSFER",
    "data_liberacao": "2026-11-09", "data_inclusao": "2026-11-09T18:06:00Z"
  }}},
  {"payload": {"value": {"codigo_curva": "9999", "nome_tipo_curva": "BASE"}}}
]
```

### `requirements.txt`

Dependências.

```text
requests
loguru
boto3
```

## gravardadoscurvas

Lambda de gravação: recebe a curva com os pilares e gera o XML do Murex a partir do template (palavra-chave `dadoslambda`, mesmo padrão da enviardadospreco).

### `xml_curva_service.py`

Monta o bloco da curva e substitui a palavra-chave no template.

```python
from xml.sax.saxutils import escape, quoteattr

PALAVRA_CHAVE = "dadoslambda"   # mesma convenção dos templates de preço no S3


def montar_bloco_curva(curva, pilares):
    """Bloco que substitui 'dadoslambda'. PROVISÓRIO: ajustar às tags reais de curva do Murex."""
    linhas = [f"<mp:curve mp:name={quoteattr(curva['nome_curva'])} mp:code={quoteattr(curva['codigo_curva'])} "
              f"mp:date={quoteattr(curva['data_liberacao'])}>"]
    for p in sorted(pilares, key=lambda p: int(p["prazo_dias"])):
        dias = int(p["prazo_dias"])
        linhas.append(f"  <mp:pillar mp:label=\"{dias}D\" mp:days=\"{dias}\" mp:rate={quoteattr(str(p['taxa']))}/>")
    linhas.append("</mp:curve>")
    return "\n".join(linhas)


def gerar_xml(template, curva, pilares):
    if template.count(PALAVRA_CHAVE) != 1:
        raise ValueError("template deve ter exatamente uma palavra-chave dadoslambda")
    return template.replace(PALAVRA_CHAVE, montar_bloco_curva(curva, pilares))
```

### `curva_template.xml`

Template XML (envelope igual ao de preços; bloco da curva provisório).

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!-- Envelope copiado do template de preço (FGEQ) da documentação. O bloco interno (palavra-chave abaixo) é PROVISÓRIO até termos um XML de curva real do Murex. -->
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

### `requirements.txt`

Dependências.

```text
loguru
```
