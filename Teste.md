## Snippet da Solução 

```import pandas as pd```

Tabela com todas as transações das máquinas de cartão de crédito realizadas pelos clientes da Acquire

``` dados_transacoes = { 'client_id':[3545, 3545, 3509, 3510, 4510], 'total_amount':[3000, 4500, 69998, 1, 341],'discount_percentage':[6.99, 0.45, 0, None, 40]} ```
``` df_transacoes = pd.DataFrame (dados_transacoes, index =[1, 2, 3, 4, 5]) ```

Assumindo a mesma premissa do enunciado (None = 0) para o id = 4

``` df_transacoes.loc[df_transacoes['discount_percentage'].isnull (),'discount_percentage'] = 0 ```


``` df_transacoes['total_liquido'] =  df_transacoes['total_amount'] * (1 - df_transacoes['discount_percentage'] / 100) ```

Tabela com os detalhes financeiros dos contratos da Acquirer LTDA com seus clientes.
 ``` dados_contratos = { 'client_id':[3545, 3545, 3509, 3510], 'client_name':['Magazine Luana', 'Magazine Luana', 'Lojas Italianas', 'Carrerfive'], 'percentage':[2.00, 1.95, 1, 3.00], 'is_active':[True, False, True, True]} ```

 ``` df_contratos = pd.DataFrame (dados_contratos, index =[3, 4, 5, 6]) ```

Considerar somente ativos

 ``` df_contratos_validos = df_contratos.loc[df_contratos['is_active'] == True] ```

unificar as tabelas...

 ``` df_merge = pd.merge (df_contratos_validos, df_transacoes, on = 'client_id') ```

 Coluna com a comissao Acquirer
 
 ``` df_merge['total_Acquirer'] =  df_merge['total_liquido'] * (df_merge['percentage'] / 100) ```

somando...

 ```print (df_merge['total_Acquirer'].sum()) ```


## Pergunta de arquitetura:  
*"Além do código acima, considere que uma escala de ~200 milhões de transações por dia e que o cálculo deverá apresentar um resultado do valor total do mês"*

Essas transações provavelmente virão de muitos clientes das mais variadas formas e formatos. A solução proposta  seria utilizar a coleta e integração desses dados em um Data LakeHouse, hospedado em uma das grandes clouds (Azure, Amazon ou Google), já contando com um versionamento dos dados em Delta.  Junto a isso, utilizando spark e kafka, constrói-se uma pipeline para tratamento de dados com ferramentas como Dremio e Databricks. Para consultar os dados de forma rápida, instantânea e eficiente, utilizaria o Trino para executar consultas em tempo real com SQL. 
