"""
Teste local da captura de curvas, sem AWS e sem instalar nada.

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
