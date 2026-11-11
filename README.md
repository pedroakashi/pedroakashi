### Hi, my name is Pedro
(Olá, me chamo Pedro)

Current,i'm studying Computer Science at IMT

(Atualmente, estou estudando Ciencia da Computação no IMT)

[![Gmail](https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white)](https://mail.google.com/mail/u/0/#inbox?compose=new)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/pedro-akashi-241b67254/)

![Pedro's GitHub stats](https://github-readme-stats.vercel.app/api?username=pedroakashi&show_icons=true&theme=dracula)




Crie duas Lambdas Python para curvas do IM8 → Murex, seguindo exatamente o padrão das Lambdas obterdadospreco e enviardadospreco deste repositório (estrutura de pastas app/src, requests, loguru, logger_config, sts_service, credentials_service, http_request_error).

1) obterdadoscurvas (copie a estrutura da obterdadospreco):
- Recebe o evento do Connector com codigo_curva, nome_curva e data_liberacao.
- Obtém o token STS e chama a API de curvas do IM8.
- Filtra só os pilares: descarta os pontos marcados como interpolados. O nome do campo vem da env CAMPO_INTERPOLADO (padrão "indicador_interpolado").
- Monta a mensagem {"curva": {...}, "pilares": [...]}. O envio para SQS fica como TODO.
- Crie um run_local.py com evento e resposta do IM8 simulados, que salve as mensagens em saida/mensagens_sqs.json.

2) gravardadoscurvas (copie o esquema de template da enviardadospreco):
- Recebe Records da SQS e gera o XML da curva com um template que tem a palavra "dadoslambda", substituída pelos dados.
- Use o mesmo envelope do XML de preço (GuiRoot / XmlCache / MarketParameters). Por dentro use, provisoriamente:
  <mp:curve mp:name="..." mp:code="..." mp:date="AAAAMMDD">
    <mp:pillar mp:label="30D" mp:days="30" mp:rate="10.7200"/>
- O destino (API do Murex ou EFS) fica como TODO.
- Crie um run_local.py que leia ../../obterdadoscurvas/app/saida/mensagens_sqs.json e grave saida/{nome_curva}_{data}.xml.

Marque com # PROVISÓRIO: as tags da curva, os campos da resposta do IM8 (prazo_dias, taxa) e o campo de interpolado.
