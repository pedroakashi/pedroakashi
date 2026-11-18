# Gravar Dados Murex Uruguai — Curvas (`gravardadoscurvas`)

Lambda que transforma as curvas capturadas no IM8 no arquivo XML do Murex, seguindo o *Guia de Implementação v1.0* (seção 8) e o padrão da `enviardadospreco` (template com a palavra-chave `dadoslambda`).
Usa só Python padrão; `boto3` apenas quando o template ou o destino for S3. Todos os arquivos ficam na mesma pasta, sem subpastas.

**Fluxo:** mensagem da SQS (enviada pela captura) → valida → template XML (S3) → substitui `dadoslambda` pelos pilares → valida o XML → grava de forma atômica no destino.

**Status:** ligada à Lambda de captura e testada localmente de ponta a ponta com dados simulados. Provisórios: tags de curva do XML (contrato Murex), nomes dos campos dos pilares (resposta real do IM8) e destino final (EFS ou API on-premises).

**Teste local:** `python teste_local.py` (usa `mensagem_teste.json`), `python teste_local.py fator` (calcula o fator) e `python teste_local.py ..\testeaws\mensagens_sqs.json` (fluxo completo com as mensagens da captura).

## Variáveis de ambiente

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

## Arquivos

### `lambda_function.py`

Handler: recebe o lote da SQS e devolve em `batchItemFailures` só as mensagens que falharam (voltam para a fila e, esgotadas as tentativas, vão para a DLQ).

```python
"""
gravardadoscurvas — Lambda "Gravar Dados Murex Uruguai" para curvas (guia, seções 3.9 a 3.12 e 8).

Fluxo: mensagem da SQS -> valida -> template XML (S3) -> substitui 'dadoslambda' pelos pilares
       -> valida o XML -> grava de forma atômica no destino (local / S3 / EFS).
Só Python padrão; boto3 apenas quando o template ou o destino for S3.
"""
from entrega_service import ErroEntrega, entregar, nome_arquivo
from logger_config import log
from mensagem_model import MensagemInvalida, ler_mensagem
from template_service import obter_template
from xml_curva_service import ErroTransformacao, gerar_xml


def processar(body):
    correlation_id, curva, pilares = ler_mensagem(body)
    template, origem = obter_template()
    conteudo = gerar_xml(template, curva, pilares)
    caminho, tamanho, hash_arquivo = entregar(nome_arquivo(curva), conteudo)
    log("INFO", "XML da curva entregue.", correlation_id=correlation_id, codigo_curva=curva["codigo_curva"],
        nome_curva=curva["nome_curva"], total_pilares=len(pilares), template=origem,
        arquivo=caminho, tamanho_bytes=tamanho, hash=hash_arquivo)
    return {"codigo_curva": curva["codigo_curva"], "status": "ENTREGUE", "arquivo": caminho,
            "tamanho_bytes": tamanho, "hash": hash_arquivo}


def lambda_handler(event, context):
    """Gatilho SQS com ReportBatchItemFailures: só as mensagens com erro voltam para a fila
    (e vão para a DLQ quando esgotarem as tentativas)."""
    registros = event.get("Records", [])
    log("INFO", "Início da lambda GravarDadosCurvas.", total_mensagens=len(registros))

    falhas, resultados = [], []
    for registro in registros:
        message_id = registro.get("messageId")
        try:
            resultados.append(processar(registro.get("body")))
        except (MensagemInvalida, ErroTransformacao, ErroEntrega) as e:
            log("ERROR", "Falha ao gravar curva.", message_id=message_id, tipo_erro=type(e).__name__, erro=str(e))
            falhas.append({"itemIdentifier": message_id})
            resultados.append({"message_id": message_id, "status": "ERRO", "erro": str(e)})
        except Exception as e:
            log("ERROR", "Erro inesperado ao gravar curva.", message_id=message_id, erro=repr(e)[:300])
            falhas.append({"itemIdentifier": message_id})
            resultados.append({"message_id": message_id, "status": "ERRO", "erro": repr(e)[:300]})

    log("INFO", "Término.", total_ok=len(registros) - len(falhas), total_erro=len(falhas))
    return {"batchItemFailures": falhas, "resultados": resultados}
```

### `logger_config.py`

Log em JSON no CloudWatch, sem biblioteca externa (igual ao da captura).

```python
import json


def log(nivel, mensagem, **campos):
    """Log em JSON no CloudWatch (sem biblioteca externa)."""
    print(json.dumps({"level": nivel, "message": mensagem, **campos}, ensure_ascii=False, default=str))
```

### `mensagem_model.py`

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

### `template_service.py`

Lê o template XML: do bucket S3 na AWS (`TEMPLATES_BUCKET`) ou do arquivo local no teste.

```python
import os

PALAVRA_CHAVE = "dadoslambda"   # mesma convenção dos templates de preço (enviardadospreco)
_cache = {}


def obter_template():
    """Lê o template XML da curva.
    - Na AWS: bucket TEMPLATES_BUCKET, chave TEMPLATES_PREFIX + TEMPLATE_CURVA (padrão do guia, seção 8).
    - Local: arquivo TEMPLATE_LOCAL (padrão curva_template.xml nesta pasta).
    """
    bucket = os.getenv("TEMPLATES_BUCKET")
    if bucket:
        chave = os.getenv("TEMPLATES_PREFIX", "templates-xml-murex/") + os.getenv("TEMPLATE_CURVA", "curva_template.xml")
        if chave not in _cache:
            import boto3
            s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION_NAME", "sa-east-1"))
            _cache[chave] = s3.get_object(Bucket=bucket, Key=chave)["Body"].read().decode("utf-8")
        return _cache[chave], f"s3://{bucket}/{chave}"

    caminho = os.getenv("TEMPLATE_LOCAL") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "curva_template.xml")
    with open(caminho, encoding="utf-8") as f:
        return f.read(), caminho
```

### `xml_curva_service.py`

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

### `entrega_service.py`

Gera o nome do arquivo no padrão do guia e grava de forma atômica, sem duplicar nem sobrescrever. Destino: `local`, `s3` ou `efs`.

```python
import errno
import hashlib
import os
import re
import uuid
from datetime import datetime


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
        import boto3
        bucket = os.getenv("SAIDA_BUCKET")
        chave = os.getenv("SAIDA_PREFIX", "curvas_xml/") + nome
        boto3.client("s3", region_name=os.getenv("AWS_REGION_NAME", "sa-east-1")).put_object(
            Bucket=bucket, Key=chave, Body=dados, ContentType="application/xml")
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

### `curva_template.xml`

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

### `teste_local.py`

Teste local sem AWS: gera os XMLs na pasta `saida`.

```python
"""
Teste local da gravardadoscurvas, sem AWS e sem instalar nada.

  python teste_local.py                                   -> usa mensagem_teste.json
  python teste_local.py fator                             -> idem, calculando também Fator = 100000 / PU
  python teste_local.py ..\testeaws\mensagens_sqs.json    -> usa as mensagens geradas pela captura (fluxo completo)

A 3ª mensagem do teste é inválida de propósito (sem pilares) para mostrar o tratamento de erro.
Rodar duas vezes mostra a idempotência: o mesmo arquivo não é duplicado nem sobrescrito.
"""
import json
import os
import sys

PASTA = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PASTA)
os.environ.setdefault("DESTINO", "local")
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

### `mensagem_teste.json`

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
