# Documentação de Cruzamentos SQL - BCadastro/GENESIS

> **Documento de Transferência de Conhecimento**
> Sistema GENESIS - Análise de Grupos Econômicos no Simples Nacional
> Receita Estadual de Santa Catarina

---

## 1. Contexto do Projeto

### 1.1 Objetivo Principal

O sistema identifica **grupos econômicos irregulares** no Simples Nacional baseado na **Lei Complementar 123/2006, Art. 3º, § 4º, Inciso IV**, que determina:

> Uma empresa do Simples Nacional é **irregular** quando um sócio participa com mais de 10% em outra empresa, e a **receita bruta global do grupo** ultrapassa **R$ 4.800.000,00 anuais**.

### 1.2 Por que foi feito assim

O fluxo de dados foi estruturado em **camadas** para:

1. **Rastreabilidade**: Cada etapa pode ser auditada independentemente
2. **Performance**: Tabelas intermediárias evitam recálculos em cada consulta
3. **Flexibilidade**: Novas análises podem ser criadas sem refazer todo o processamento
4. **Manutenibilidade**: Correções podem ser aplicadas em pontos específicos

---

## 2. Arquitetura de Dados

### 2.1 Visão Geral do Fluxo

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          TABELAS ORIGINAIS (Fonte)                      │
├─────────────────────────────────────────────────────────────────────────┤
│  bcadastro_base_cnpj_completo     → Cadastro de empresas (CNPJ)        │
│  bcadastro_base_socios_consolidado → Sócios das empresas (CPF)         │
│  bcadastro_pgdas_consolidado      → Declarações PGDAS-D                │
│  bcadastro_tab_raiz_cpf_pai       → Histórico RBA por CPF              │
│  feitoza_base_periodos_sn         → Períodos no Simples Nacional       │
│  feitoza_rba_12_meses             → RBA acumulada 12 meses             │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      TABELAS INTERMEDIÁRIAS (Processamento)             │
├─────────────────────────────────────────────────────────────────────────┤
│  feitoza_grupos_identificados     → Grupos econômicos por CPF          │
│  feitoza_rba_grupo                → RBA consolidada por grupo          │
│  feitoza_fato_gerador             → Momento da violação do limite      │
│  feitoza_resumo_grupos_irregulares → Resumo para fiscalização          │
│  feitoza_lista_acao_fiscal        → Lista priorizada para ação         │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         TABELA FINAL (Output)                           │
├─────────────────────────────────────────────────────────────────────────┤
│  bcadastro_output_final_acl       → Resultado consolidado p/ dashboard │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Banco de Dados

| Parâmetro | Valor |
|-----------|-------|
| **Sistema** | Apache Impala |
| **Database** | `gessimples` |
| **Host** | `bdaworkernode02.sef.sc.gov.br` |
| **Porta** | 21050 |
| **Autenticação** | LDAP |
| **SSL** | Habilitado |

---

## 3. Descrição Detalhada das Tabelas

### 3.1 Tabelas Originais (Fonte de Dados)

#### 3.1.1 `bcadastro_base_cnpj_completo`
**Propósito**: Base completa de empresas cadastradas

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `cnpj_raiz` | STRING | 8 primeiros dígitos do CNPJ (identifica grupo empresarial) |
| `cnpj_completo` | STRING | CNPJ completo (14 dígitos) |
| `cnpj_matriz` | STRING | Flag se é matriz (S/N) |
| `razao_social` | STRING | Nome da empresa |
| `natureza_juridica_desc` | STRING | Tipo jurídico (LTDA, SA, MEI, etc.) |
| `porte_empresa` | STRING | ME, EPP, DEMAIS |
| `capital_social` | DOUBLE | Capital social declarado |
| `situacao_cadastral_desc` | STRING | ATIVA, BAIXADA, SUSPENSA, etc. |
| `uf` | STRING | Estado da empresa |

#### 3.1.2 `bcadastro_base_socios_consolidado`
**Propósito**: Relação de sócios/titulares das empresas

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `cnpj_raiz` | STRING | Empresa que o sócio participa |
| `cpf` | STRING | CPF do sócio (11 dígitos) |
| `qualificacao` | STRING | Tipo de participação (SOCIO, ADM, TITULAR, etc.) |
| `dt_ini_resp` | DATE | Data de início da responsabilidade |
| `socio_ou_titular` | STRING | Flag S/N |
| `tipo_socio` | STRING | PF (pessoa física) ou PJ |
| `uf` | STRING | Estado do vínculo |

#### 3.1.3 `bcadastro_pgdas_consolidado`
**Propósito**: Declarações mensais do PGDAS-D (receitas do Simples)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `cnpj_raiz` | STRING | Empresa declarante |
| `cnpj_completo` | STRING | CNPJ completo |
| `periodo_apuracao` | INT | Período YYYYMM (ex: 202301) |
| `pa` | STRING | Período no formato texto |
| `vl_rpa_int` | DOUBLE | Receita bruta do período |
| `vl_icms` | DOUBLE | ICMS apurado |
| `vl_ativ` | DOUBLE | Valor de atividade |
| `uf` | STRING | Estado |

#### 3.1.4 `bcadastro_tab_raiz_cpf_pai`
**Propósito**: Histórico de RBA por CPF com vinculações a grupos

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `cpf` | STRING | CPF do sócio responsável |
| `cnpj_raiz` | STRING | Empresa vinculada |
| `pa` | STRING | Período de apuração |
| `vl_rba_pgdas` | DOUBLE | RBA declarada no período |
| `vl_icms_12m` | DOUBLE | ICMS acumulado 12 meses |
| `qualificacao` | STRING | Qualificação do sócio |
| `uf` | STRING | Estado |
| `num_grupo` | INT | Número do grupo econômico |
| `qte_cnpj` | INT | Quantidade de CNPJs no grupo |

#### 3.1.5 `feitoza_base_periodos_sn`
**Propósito**: Períodos de permanência no Simples Nacional

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `cnpj_raiz` | STRING | Empresa |
| `dt_ini_regime` | DATE | Data de entrada no Simples |
| `dt_fim_regime` | DATE | Data de saída (NULL se ainda ativo) |
| `periodo_inicio_sn` | INT | Período início (YYYYMM) |
| `periodo_fim_sn` | INT | Período fim (YYYYMM) |

#### 3.1.6 `feitoza_rba_12_meses`
**Propósito**: RBA acumulada dos últimos 12 meses

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `cnpj_raiz` | STRING | Empresa |
| `cpf` | STRING | Sócio responsável |
| `vl_rba_12m` | DOUBLE | Receita bruta acumulada 12 meses |
| `pa` | STRING | Período de referência |
| `dt_carga` | DATE | Data de atualização |

---

### 3.2 Tabelas Intermediárias (Processamento)

#### 3.2.1 `feitoza_grupos_identificados`
**Propósito**: Identificar grupos econômicos por vínculo societário

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `num_grupo` | INT | Identificador único do grupo |
| `cpf` | STRING | CPF que vincula as empresas |
| `qte_cnpj` | INT | Quantidade de CNPJs no grupo |
| `qte_socio` | INT | Quantidade de sócios |

**Regra de Formação**: Um CPF que participa em 2+ empresas forma um grupo.

#### 3.2.2 `feitoza_rba_grupo`
**Propósito**: RBA consolidada de todo o grupo econômico

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `num_grupo` | INT | Grupo econômico |
| `cpf` | STRING | CPF responsável |
| `vl_rba_grupo` | DOUBLE | Soma da RBA de todas empresas do grupo |
| `pa` | STRING | Período |
| `qte_cnpj` | INT | Empresas no grupo |

#### 3.2.3 `feitoza_fato_gerador`
**Propósito**: Identificar o momento em que o limite foi ultrapassado

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `cpf` | STRING | CPF responsável |
| `num_grupo` | INT | Grupo econômico |
| `pa_fato` | STRING | Período do fato gerador (YYYYMM) |
| `vl_rba_grupo` | DOUBLE | RBA no momento do fato |
| `limite_anual` | DOUBLE | Limite vigente (R$ 4.800.000,00) |

#### 3.2.4 `bcadastro_output_final_acl`
**Propósito**: Tabela final consolidada para o dashboard

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `num_grupo` | INT | Identificador do grupo |
| `cpf` | STRING | CPF responsável pelo vínculo |
| `cnpj_raiz` | STRING | CNPJ raiz da empresa |
| `razao_social` | STRING | Nome da empresa |
| `acao` | STRING | Ação fiscal recomendada |
| `vl_ct` | DOUBLE | Crédito tributário (ICMS + juros + multa) |
| `receita_pa_fato` | DOUBLE | Receita no período do fato |
| `qte_cnpj` | INT | Quantidade de empresas no grupo |
| `qte_socio` | INT | Quantidade de sócios |
| `flag_periodo` | STRING | Classificação do período |
| `emite_te_sc` | STRING | Deve emitir Termo de Exclusão (S/N) |
| `uf` | STRING | Estado da empresa |
| `qualificacao` | STRING | Qualificação do sócio |
| `dt_fato` | DATE | Data do fato gerador |

---

## 4. Regras de Negócio

### 4.1 Identificação de Grupos Econômicos

**Regra Principal**: Quando um CPF participa com **mais de 10%** do capital social em **2 ou mais empresas**, estas formam um **grupo econômico**.

```
┌──────────┐     ┌──────────────┐     ┌──────────┐
│Empresa A │◄────│  CPF: 123...  │────►│Empresa B │
│CNPJ: xxx │     │  (15% em A)  │     │CNPJ: yyy │
└──────────┘     │  (20% em B)  │     └──────────┘
                 └──────────────┘
                       │
                       ▼
              GRUPO ECONÔMICO FORMADO
              (num_grupo = 1)
```

### 4.2 Limites do Simples Nacional

| Parâmetro | Valor | Base Legal |
|-----------|-------|------------|
| **Limite Anual** | R$ 4.800.000,00 | LC 123/2006 |
| **Limite Proporcional** | R$ 5.760.000,00 (80% para empresas novas) | Resolução CGSN |

### 4.3 Fato Gerador (PA_FATO)

O **fato gerador** é o primeiro período em que a RBA acumulada do grupo ultrapassa o limite:

```sql
-- Identificação do fato gerador
SELECT
    cpf,
    num_grupo,
    MIN(pa) as pa_fato  -- Primeiro período acima do limite
FROM feitoza_rba_grupo
WHERE vl_rba_grupo > 4800000.00
GROUP BY cpf, num_grupo
```

**Hierarquia de Períodos** (ordem de prioridade):
1. 2021
2. 2022
3. 2023
4. 2024
5. 2025

### 4.4 Classificação de Ações Fiscais

| Ação | Critério | Interesse SC | Emite TE |
|------|----------|--------------|----------|
| **EXCLUSAO_COM_DEBITO** | Empresa em SC + RBA > limite + débitos pendentes | Sim | Sim |
| **EXCLUSAO_SEM_DEBITO** | Empresa em SC + RBA > limite + sem débitos | Sim | Sim (preventivo) |
| **SEM_INTERESSE** | Empresa fora de SC OU já excluída | Não | Não |

### 4.5 Qualificação de Sócios

Valores padronizados para o campo `qualificacao`:

| Qualificação | Descrição |
|--------------|-----------|
| SOCIO | Sócio comum |
| SOCIO_ADM | Sócio administrador |
| DIRETOR | Diretor da empresa |
| PRESIDENTE | Presidente |
| ADM | Administrador |
| TITULAR | Titular (empresário individual) |
| SOCIO_GERENTE | Sócio com poderes de gerência |

**Regra SOCIO_OU_TITULAR = 'S'**:
- Tipo de sócio é PF (pessoa física)
- Qualificação está em: SOCIO, SOCIO_ADM, SOCIO_C_CAPITAL, SOCIO_EX, SOCIO_INCAPAZ, SOCIO_MENOR, SOCIO_S_CAPITAL, TITULAR

---

## 5. Queries SQL do Dashboard

### 5.1 Resumo Geral (KPIs)

```sql
SELECT
    COUNT(DISTINCT num_grupo) as total_grupos,
    COUNT(DISTINCT cnpj_raiz) as total_empresas,
    COUNT(DISTINCT cpf) as total_socios,
    SUM(CASE WHEN acao = 'EXCLUSAO_COM_DEBITO' THEN 1 ELSE 0 END) as exclusao_com_debito,
    SUM(CASE WHEN acao = 'EXCLUSAO_SEM_DEBITO' THEN 1 ELSE 0 END) as exclusao_sem_debito,
    SUM(CASE WHEN acao = 'SEM_INTERESSE' THEN 1 ELSE 0 END) as sem_interesse,
    SUM(vl_ct) as credito_total,
    AVG(vl_ct) as credito_medio,
    MAX(vl_ct) as credito_maximo,
    SUM(CASE WHEN emite_te_sc = 'S' THEN 1 ELSE 0 END) as emite_te_sc,
    SUM(receita_pa_fato) as receita_total,
    AVG(receita_pa_fato) as receita_media
FROM gessimples.bcadastro_output_final_acl
```

### 5.2 Distribuição por Ação Fiscal

```sql
SELECT
    acao,
    COUNT(DISTINCT num_grupo) as qtd_grupos,
    COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
    SUM(vl_ct) as credito_total,
    AVG(vl_ct) as credito_medio,
    AVG(receita_pa_fato) as receita_media,
    MAX(receita_pa_fato) as receita_maxima
FROM gessimples.bcadastro_output_final_acl
GROUP BY acao
ORDER BY qtd_grupos DESC
```

### 5.3 Distribuição por Período

```sql
SELECT
    flag_periodo,
    COUNT(DISTINCT num_grupo) as qtd_grupos,
    COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
    SUM(vl_ct) as credito_total,
    AVG(vl_ct) as credito_medio
FROM gessimples.bcadastro_output_final_acl
WHERE flag_periodo IS NOT NULL AND flag_periodo != ''
GROUP BY flag_periodo
ORDER BY qtd_grupos DESC
```

### 5.4 Distribuição por Estado (UF)

```sql
SELECT
    uf,
    COUNT(DISTINCT num_grupo) as qtd_grupos,
    COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
    SUM(vl_ct) as credito_total,
    AVG(vl_ct) as credito_medio,
    SUM(CASE WHEN emite_te_sc = 'S' THEN 1 ELSE 0 END) as emite_te,
    SUM(CASE WHEN acao = 'EXCLUSAO_COM_DEBITO' THEN 1 ELSE 0 END) as exclusao_debito
FROM gessimples.bcadastro_output_final_acl
GROUP BY uf
ORDER BY qtd_empresas DESC
```

### 5.5 Top 100 Grupos por Crédito Tributário

```sql
SELECT
    num_grupo,
    cpf,
    qte_cnpj,
    qte_socio,
    SUM(vl_ct) as vl_ct_total,
    MAX(receita_pa_fato) as receita_maxima,
    MAX(acao) as acao_principal,
    MAX(flag_periodo) as periodo_principal,
    COUNT(DISTINCT cnpj_raiz) as empresas_grupo,
    COUNT(DISTINCT CASE WHEN uf = 'SC' THEN cnpj_raiz END) as empresas_sc,
    COUNT(DISTINCT CASE WHEN emite_te_sc = 'S' THEN cnpj_raiz END) as te_emitir
FROM gessimples.bcadastro_output_final_acl
WHERE vl_ct > 0
GROUP BY num_grupo, cpf, qte_cnpj, qte_socio
ORDER BY vl_ct_total DESC
LIMIT 100
```

### 5.6 Detalhes de um Grupo Específico

```sql
SELECT *
FROM gessimples.bcadastro_output_final_acl
WHERE num_grupo = {num_grupo}
ORDER BY vl_ct DESC, uf, razao_social
```

### 5.7 Detalhes de uma Empresa

```sql
-- Dados cadastrais
SELECT *
FROM gessimples.bcadastro_base_cnpj_completo
WHERE cnpj_raiz = '{cnpj_raiz}'

-- Sócios da empresa
SELECT *
FROM gessimples.bcadastro_base_socios_consolidado
WHERE cnpj_raiz = '{cnpj_raiz}'
ORDER BY socio_ou_titular DESC
```

### 5.8 Histórico de RBA por CPF

```sql
SELECT
    pa,
    COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
    SUM(vl_rba_pgdas) as rba_total,
    AVG(vl_rba_pgdas) as rba_media,
    SUM(vl_icms_12m) as icms_total
FROM gessimples.bcadastro_tab_raiz_cpf_pai
WHERE cpf = '{cpf}'
GROUP BY pa
ORDER BY pa
```

### 5.9 PGDAS de uma Empresa

```sql
SELECT *
FROM gessimples.bcadastro_pgdas_consolidado
WHERE cnpj_raiz = '{cnpj_raiz}'
  AND periodo_apuracao >= {periodo_inicio}
  AND periodo_apuracao <= {periodo_fim}
ORDER BY periodo_apuracao DESC
```

---

## 6. Parâmetros de Configuração

### 6.1 Parâmetros do Dashboard (BCADASTRO.py)

```python
# Conexão com banco de dados
IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'gessimples'

# Autenticação (arquivo secrets.toml)
IMPALA_USER = st.secrets["impala_credentials"]["user"]
IMPALA_PASSWORD = st.secrets["impala_credentials"]["password"]

# Cache
TTL_AGREGADOS = 3600      # 1 hora para dados resumidos
TTL_DETALHES = 1800       # 30 minutos para detalhes

# Limites de consulta
TOP_GRUPOS_LIMITE = 100   # Máximo de grupos no ranking
TOP_EMPRESAS_LIMITE = 2000 # Máximo de empresas na busca
TOP_QUALIFICACOES = 15    # Limite de qualificações distintas
```

### 6.2 Valores de Negócio

```python
# Limites fiscais (LC 123/2006)
LIMITE_ANUAL_SN = 4800000.00          # R$ 4.800.000,00
LIMITE_PROPORCIONAL = 5760000.00      # R$ 5.760.000,00 (80%)

# Período de análise
PERIODO_INICIO = '202101'             # Janeiro/2021
PERIODO_FIM = '202512'                # Dezembro/2025

# Estados de interesse
ESTADO_PRINCIPAL = 'SC'               # Santa Catarina
```

---

## 7. Tratamentos Especiais

### 7.1 Deduplicação de Registros

No carregamento de detalhes do grupo, há registros duplicados que precisam ser tratados:

```python
# Critério de deduplicação: Manter apenas 1 registro por CNPJ
# Prioridade: Maior VL_CT → Maior RECEITA_PA_FATO → DT_FATO mais recente

df = df.sort_values(
    ['cnpj_raiz', 'vl_ct', 'receita_pa_fato', 'dt_fato'],
    ascending=[True, False, False, False]
)
df = df.drop_duplicates(subset=['cnpj_raiz'], keep='first')
```

### 7.2 Filtros de Exclusão (ACL Format)

Registros que são **excluídos** da análise:

1. Empresas com `situacao_cadastral_desc = 'BAIXADA'` antes de 2020
2. Períodos no Simples Nacional terminados antes de 2019-12-31
3. Sócios que saíram da empresa (`dt_fim_resp IS NOT NULL`)
4. Empresas que nunca foram do Simples Nacional

### 7.3 Tratamento de Valores Nulos

```sql
-- Exemplo de tratamento de nulos nas agregações
WHERE flag_periodo IS NOT NULL AND flag_periodo != ''
WHERE qualificacao IS NOT NULL AND qualificacao != ''
```

---

## 8. Diagrama de Cruzamentos (JOINs)

```
bcadastro_base_socios_consolidado (s)
           │
           │ JOIN: s.cnpj_raiz = bc.cnpj_raiz
           ▼
bcadastro_base_cnpj_completo (bc)
           │
           │ JOIN: s.cnpj_raiz = psn.cnpj_raiz
           ▼
feitoza_base_periodos_sn (psn)
           │
           │ JOIN: r.pa = lim.periodo_apuracao
           ▼
bcadastro_ref_limite_sn (lim) [Tabela de limites por período]
           │
           │ JOIN: r.cpf = fg.cpf AND r.num_grupo = fg.num_grupo
           ▼
feitoza_fato_gerador (fg)
           │
           │ Consolidação final
           ▼
bcadastro_output_final_acl
```

---

## 9. Glossário de Termos

| Termo | Significado |
|-------|-------------|
| **RBA** | Receita Bruta Acumulada (12 meses) |
| **PA** | Período de Apuração (formato YYYYMM) |
| **VL_CT** | Valor do Crédito Tributário |
| **TE** | Termo de Exclusão |
| **PGDAS-D** | Programa Gerador do Documento de Arrecadação do Simples |
| **ACL** | Auditoria Compartilhada de Dados |
| **CNPJ Raiz** | 8 primeiros dígitos do CNPJ |
| **Fato Gerador** | Momento em que ocorreu a violação do limite |
| **num_grupo** | Identificador único do grupo econômico |

---

## 10. Próximos Passos Sugeridos

1. **Validar tabelas fonte**: Execute `DESCRIBE FORMATTED` em cada tabela
2. **Verificar volumes**: COUNT(*) em cada tabela para entender cardinalidade
3. **Testar joins**: Execute joins incrementalmente para validar dados
4. **Revisar limites**: Confirmar se R$ 4.800.000 está atualizado
5. **Verificar períodos**: Confirmar range de datas disponível

---

## 11. Arquivos de Referência

| Arquivo | Descrição |
|---------|-----------|
| `BCADASTRO.py` | Aplicativo principal Streamlit |
| `queries_exemplo.sql` | Queries SQL de teste |
| `BCADASTRO-Exemplo.ipynb` | Notebook com exemplos |
| `gerar_data_schemas.ipynb` | Gerador de schemas das tabelas |
| `README.md` | Documentação geral |
| `NOTAS_TECNICAS.md` | Notas sobre PySpark/Python |

---

## 12. Contato

Para dúvidas sobre esta documentação ou o sistema:
- **Autor Original**: Tiago S. Severo
- **Organização**: Receita Estadual de Santa Catarina
- **Sistema**: GENESIS v2.0

---

*Documento gerado em: 27/11/2025*
