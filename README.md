# Dependências (`requirements.txt`)

As duas Lambdas de curvas usam **só a biblioteca padrão do Python**. O `boto3` (Secrets Manager, SQS e S3) já vem instalado no runtime Python da AWS Lambda, então não precisa ser empacotado. Os arquivos existem para seguir o padrão do repositório e documentar isso.

| Lambda | Dependências externas | `boto3` usado para |
|---|---|---|
| Captura (`testeaws` / obterdadoscurvas) | Nenhuma | Secrets Manager (`InfraSecretsMX3`) e SQS de curvas |
| Gravar dados (`gravardadoscurvas`) | Nenhuma | S3, só quando o template ou o destino for S3 |

Para rodar localmente com chamadas reais à AWS (fora da Lambda): `pip install boto3`. Os testes locais (`teste_local.py`) não precisam de nada instalado.

## `testeaws/requirements.txt`

```text
# Lambda de captura de curvas (obterdadoscurvas / testeaws)
#
# Sem dependências externas: o código usa só a biblioteca padrão do Python.
# boto3 (Secrets Manager e SQS) já vem instalado no runtime Python da AWS Lambda,
# por isso não precisa ser empacotado.
#
# Para rodar fora da AWS com chamadas reais ao Secrets Manager/SQS, instalar:
# boto3>=1.28
```

## `gravardadoscurvas/requirements.txt`

```text
# Lambda de gravar dados de curvas (gravardadoscurvas)
#
# Sem dependências externas: o código usa só a biblioteca padrão do Python.
# boto3 (só quando o template ou o destino for S3) já vem instalado no runtime
# Python da AWS Lambda, por isso não precisa ser empacotado.
#
# Para rodar fora da AWS lendo/gravando no S3, instalar:
# boto3>=1.28
```
