## Teste local (`run_local.py`)

Scripts para rodar as Lambdas localmente, sem AWS e sem IM8 (tudo simulado). Funcionam mesmo sem `loguru`, `requests` e `boto3` instalados: nesse caso usam substitutos locais.

Ordem: rode primeiro o da captura (gera `mensagens_sqs.json`) e depois o da gravação (gera os XMLs).

### `obterdadospreco/run_local.py` (captura)

Como rodar:

```
cd obterdadospreco
python run_local.py
```

```python
"""Roda a Lambda localmente com o evento de teste, SEM chamar a AWS nem o IM8 (tudo simulado).

Uso (dentro da pasta app):  python run_local.py
"""
import json
import os
import sys
import types
from types import SimpleNamespace
from unittest.mock import patch

# ---------------------------------------------------------------------------
# Substitutos para rodar em máquina bloqueada (sem pip install).
# Só são usados se a biblioteca real NÃO estiver instalada. Não afetam a Lambda na AWS.
# ---------------------------------------------------------------------------

def _instalar_substitutos():
    try:
        import loguru  # noqa: F401
    except ImportError:
        class _Logger:
            def _log(self, nivel, mensagem, **campos):
                extras = " ".join(f"{k}={v}" for k, v in campos.items() if k != "extra")
                print(f"{nivel:<8} | {mensagem} {extras}".rstrip())

            def info(self, mensagem, *args, **campos):
                self._log("INFO", mensagem, **campos)

            def warning(self, mensagem, *args, **campos):
                self._log("WARNING", mensagem, **campos)

            def error(self, mensagem, *args, **campos):
                self._log("ERROR", mensagem, **campos)

            def exception(self, mensagem, *args, **campos):
                self._log("ERROR", mensagem, **campos)

            def debug(self, mensagem, *args, **campos):
                pass

            def remove(self, *args, **kwargs):
                pass

            def add(self, *args, **kwargs):
                pass

            def configure(self, *args, **kwargs):
                pass

        modulo = types.ModuleType("loguru")
        modulo.logger = _Logger()
        sys.modules["loguru"] = modulo
        print("[aviso] loguru não instalado: usando substituto local.")

    try:
        import requests  # noqa: F401
    except ImportError:
        requests_mod = types.ModuleType("requests")
        adapters = types.ModuleType("requests.adapters")

        class Session:
            def mount(self, *args, **kwargs):
                pass

            def get(self, *args, **kwargs):
                raise RuntimeError("requests não instalado: a chamada real ao IM8 não funciona aqui")

        class HTTPAdapter:
            def __init__(self, *args, **kwargs):
                pass

        class Retry:
            def __init__(self, *args, **kwargs):
                pass

        requests_mod.Session = Session
        adapters.HTTPAdapter = HTTPAdapter
        adapters.Retry = Retry
        requests_mod.adapters = adapters
        sys.modules["requests"] = requests_mod
        sys.modules["requests.adapters"] = adapters
        print("[aviso] requests não instalado: usando substituto local.")

    try:
        import boto3  # noqa: F401
    except ImportError:
        boto3_mod = types.ModuleType("boto3")
        sessao = types.ModuleType("boto3.session")
        botocore = types.ModuleType("botocore")
        excecoes = types.ModuleType("botocore.exceptions")

        class Session:
            def client(self, *args, **kwargs):
                raise RuntimeError("boto3 não instalado: o Secrets Manager real não funciona aqui")

        class ClientError(Exception):
            pass

        sessao.Session = Session
        boto3_mod.session = sessao
        excecoes.ClientError = ClientError
        botocore.exceptions = excecoes
        sys.modules.update({"boto3": boto3_mod, "boto3.session": sessao,
                            "botocore": botocore, "botocore.exceptions": excecoes})
        print("[aviso] boto3 não instalado: usando substituto local.")


_instalar_substitutos()

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
```

### `gravardadospreco/run_local.py` (gravação do XML)

Como rodar:

```
cd ..\gravardadospreco
python run_local.py ..\obterdadospreco\mensagens_sqs.json
```

```python
"""Gera o XML das curvas a partir das mensagens salvas pela captura (../obterdadoscurvas/mensagens_sqs.json)."""
import json
import os
import sys
import types

# ---------------------------------------------------------------------------
# Substitutos para rodar em máquina bloqueada (sem pip install).
# Só são usados se a biblioteca real NÃO estiver instalada. Não afetam a Lambda na AWS.
# ---------------------------------------------------------------------------

def _instalar_substitutos():
    try:
        import loguru  # noqa: F401
    except ImportError:
        class _Logger:
            def _log(self, nivel, mensagem, **campos):
                extras = " ".join(f"{k}={v}" for k, v in campos.items() if k != "extra")
                print(f"{nivel:<8} | {mensagem} {extras}".rstrip())

            def info(self, mensagem, *args, **campos):
                self._log("INFO", mensagem, **campos)

            def warning(self, mensagem, *args, **campos):
                self._log("WARNING", mensagem, **campos)

            def error(self, mensagem, *args, **campos):
                self._log("ERROR", mensagem, **campos)

            def exception(self, mensagem, *args, **campos):
                self._log("ERROR", mensagem, **campos)

            def debug(self, mensagem, *args, **campos):
                pass

            def remove(self, *args, **kwargs):
                pass

            def add(self, *args, **kwargs):
                pass

            def configure(self, *args, **kwargs):
                pass

        modulo = types.ModuleType("loguru")
        modulo.logger = _Logger()
        sys.modules["loguru"] = modulo
        print("[aviso] loguru não instalado: usando substituto local.")


_instalar_substitutos()

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
```
