import os


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
    return pontos, pilares, campo
