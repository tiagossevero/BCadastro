-- ============================================================================
-- QUERIES DE EXEMPLO - Data Schemas BCadastro
-- ============================================================================
-- Use estas queries para testar manualmente cada tabela
-- Copie e cole no seu ambiente PySpark/Impala
-- ============================================================================

-- ----------------------------------------------------------------------------
-- TABELAS ORIGINAIS
-- ----------------------------------------------------------------------------

-- 1. bcadastro_base_cnpj_completo
DESCRIBE FORMATTED gessimples.bcadastro_base_cnpj_completo;
SELECT * FROM gessimples.bcadastro_base_cnpj_completo LIMIT 10;

-- 2. bcadastro_base_socios_consolidado
DESCRIBE FORMATTED gessimples.bcadastro_base_socios_consolidado;
SELECT * FROM gessimples.bcadastro_base_socios_consolidado LIMIT 10;

-- 3. bcadastro_pgdas_consolidado
DESCRIBE FORMATTED gessimples.bcadastro_pgdas_consolidado;
SELECT * FROM gessimples.bcadastro_pgdas_consolidado LIMIT 10;

-- 4. bcadastro_tab_raiz_cpf_pai
DESCRIBE FORMATTED gessimples.bcadastro_tab_raiz_cpf_pai;
SELECT * FROM gessimples.bcadastro_tab_raiz_cpf_pai LIMIT 10;

-- 5. feitoza_base_periodos_sn
DESCRIBE FORMATTED gessimples.feitoza_base_periodos_sn;
SELECT * FROM gessimples.feitoza_base_periodos_sn LIMIT 10;

-- 6. feitoza_rba_12_meses
DESCRIBE FORMATTED gessimples.feitoza_rba_12_meses;
SELECT * FROM gessimples.feitoza_rba_12_meses LIMIT 10;


-- ----------------------------------------------------------------------------
-- TABELAS INTERMEDIÁRIAS
-- ----------------------------------------------------------------------------

-- 7. bcadastro_output_final_acl (PRINCIPAL - Output Final)
DESCRIBE FORMATTED gessimples.bcadastro_output_final_acl;
SELECT * FROM gessimples.bcadastro_output_final_acl LIMIT 10;

-- 8. feitoza_grupos_identificados
DESCRIBE FORMATTED gessimples.feitoza_grupos_identificados;
SELECT * FROM gessimples.feitoza_grupos_identificados LIMIT 10;

-- 9. feitoza_rba_grupo
DESCRIBE FORMATTED gessimples.feitoza_rba_grupo;
SELECT * FROM gessimples.feitoza_rba_grupo LIMIT 10;

-- 10. feitoza_fato_gerador
DESCRIBE FORMATTED gessimples.feitoza_fato_gerador;
SELECT * FROM gessimples.feitoza_fato_gerador LIMIT 10;

-- 11. feitoza_resumo_grupos_irregulares
DESCRIBE FORMATTED gessimples.feitoza_resumo_grupos_irregulares;
SELECT * FROM gessimples.feitoza_resumo_grupos_irregulares LIMIT 10;

-- 12. feitoza_lista_acao_fiscal
DESCRIBE FORMATTED gessimples.feitoza_lista_acao_fiscal;
SELECT * FROM gessimples.feitoza_lista_acao_fiscal LIMIT 10;


-- ============================================================================
-- QUERIES ADICIONAIS ÚTEIS
-- ============================================================================

-- Listar todas as tabelas do database
SHOW TABLES IN gessimples;

-- Ver apenas tabelas bcadastro_*
SHOW TABLES IN gessimples LIKE 'bcadastro*';

-- Ver apenas tabelas feitoza_*
SHOW TABLES IN gessimples LIKE 'feitoza*';

-- Contar registros de uma tabela
SELECT COUNT(*) as total_registros
FROM gessimples.bcadastro_output_final_acl;

-- Ver schema simplificado (sem metadata)
DESCRIBE gessimples.bcadastro_base_cnpj_completo;

-- ============================================================================
-- EQUIVALENTE EM PYSPARK
-- ============================================================================

/*
# Para executar no PySpark/Jupyter Notebook:

# DESCRIBE FORMATTED
df_describe = spark.sql("DESCRIBE FORMATTED gessimples.bcadastro_base_cnpj_completo")
df_describe.show(100, truncate=False)

# SELECT SAMPLE
df_sample = spark.sql("SELECT * FROM gessimples.bcadastro_base_cnpj_completo LIMIT 10")
df_sample.show(truncate=False)

# Converter para Pandas para melhor visualização
df_pandas = df_sample.toPandas()
print(df_pandas)

# Salvar em CSV
df_pandas.to_csv('bcadastro_base_cnpj_completo_sample.csv', index=False)
*/


-- ============================================================================
-- VERIFICAÇÕES DE QUALIDADE DOS DADOS
-- ============================================================================

-- Verificar se tabelas estão vazias
SELECT
    'bcadastro_base_cnpj_completo' as tabela,
    COUNT(*) as total_registros
FROM gessimples.bcadastro_base_cnpj_completo
UNION ALL
SELECT
    'bcadastro_output_final_acl' as tabela,
    COUNT(*) as total_registros
FROM gessimples.bcadastro_output_final_acl;

-- Verificar distribuição por UF (exemplo)
SELECT
    uf,
    COUNT(*) as total
FROM gessimples.bcadastro_base_cnpj_completo
GROUP BY uf
ORDER BY total DESC
LIMIT 10;

-- Verificar períodos disponíveis (exemplo)
SELECT
    MIN(periodo_apuracao) as periodo_inicial,
    MAX(periodo_apuracao) as periodo_final,
    COUNT(DISTINCT periodo_apuracao) as total_periodos
FROM gessimples.feitoza_rba_12_meses;
