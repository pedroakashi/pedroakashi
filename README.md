import uuid
from logger_config import logger
from credentials_service import obtain_client_credentials
from sts_service import StsService
from im8_curvas_service import Im8CurvasService
from curva_event_model import CurvaEventModel, EventoInvalido
from pilares_service import filtrar_pilares


def lambda_handler(event, context):
    request_id = getattr(context, "aws_request_id", "local")
    correlation_id = str(uuid.uuid1())
    logger.configure(extra={"correlation_id": correlation_id, "aws_request_id": request_id})
    logger.info("Início da lambda ObterDadosCurvas.", total_eventos=len(event))

    # Mesmo padrão da obterdadospreco: credenciais do InfraSecretsMX3 + token do STS (1x por invocação)
    credentials_model = obtain_client_credentials()
    sts_token = StsService().obtain_sts_token(credentials_model)
    im8_service = Im8CurvasService(correlation_id, credentials_model.client_id)

    resultados = []
    for elemento in event:
        try:
            curva = CurvaEventModel.from_event(elemento["payload"]["value"])
        except (EventoInvalido, KeyError, TypeError) as e:
            logger.error("Evento de curva inválido, descartado.", erro=str(e))
            resultados.append({"status": "INVALIDO"})
            continue

        logger.info("Curva recebida.", codigo_curva=curva.codigo_curva, nome_curva=curva.nome_curva,
                    tipo_curva=curva.tipo_curva, data_liberacao=curva.data_liberacao)

        resposta = im8_service.obter_pontos(sts_token, curva)
        pilares = filtrar_pilares(resposta)

        # Mensagem que irá para a SQS de curvas (envio ainda não implementado: falta a fila).
        mensagem = {"curva": curva.__dict__, "pilares": pilares}
        logger.info("Curva capturada.", codigo_curva=curva.codigo_curva, total_pilares=len(pilares))
        resultados.append({"codigo_curva": curva.codigo_curva, "status": "CAPTURADA",
                           "total_pilares": len(pilares), "mensagem_sqs": mensagem})

    logger.info("Término com sucesso.", resultados=resultados)
    return resultados
