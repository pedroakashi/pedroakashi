Bom dia, Well, tudo bem?

Estou preenchendo o chamado de acesso à API de curvas no Shared – Preços e Cálculos. Montei um rascunho do que pretendo preencher, você consegue dar uma olhada e me ajudar com os campos que faltam?

Faltam (dados da squad/sigla):
1. Centro de Custo
2. E-mail do responsável pelo centro de custo (GPM)
3. Grupo de e-mail da squad / responsável pela sigla MX3 (a Letícia comentou que pode ser um gerente)

Rascunho dos outros campos:

- Está desligando algo do legado? Não

- O que deseja consumir?
API de Curvas do IM8 (somente leitura), para a sigla consumidora MX3: cadastro de curvas (cadastrocurvas) e pontos das curvas pricing e transfer (pontoscurvas e pontotransferencias). Provedores: CURVA_CADASTRO_PRICING_RO, CURVA_CADASTRO_TRANSFER_RO, CURVA_PONTO_PRICING_RO, ASSET_CURVAS e CURVA_PONTOS_TRANSFER_RO. Liberação somente em ambiente dev neste momento; hom e prod serão solicitados em novo chamado.

- Em qual horário deseja realizar o consumo?
Diariamente, de forma automática, a cada publicação de curva no IM8 (acionado pelos eventos de curvas divulgadas), principalmente no fim do dia após o fechamento das curvas. (Vocês costumam colocar algum horário específico?)

- Qual a justificativa para o consumo?
Integração das curvas de juros do IM8 com o Murex do Itaú Uruguai (projeto Murex Uruguai). A Lambda de captura da sigla MX3 consome a API de Curvas para obter o cadastro e os pilares das curvas e gerar os arquivos de carga no Murex, no mesmo modelo já utilizado no fluxo de preços.

Se algo estiver diferente do que vocês costumam colocar, me avisa que eu ajusto. Obrigado!
