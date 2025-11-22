# 📚 Instruções - Gerador de Data Schemas

Este documento explica como gerar automaticamente os data-schemas de todas as tabelas do projeto BCadastro.

---

## 🎯 O que o script faz?

O script `gerar_data_schemas.py` executa automaticamente:

1. **DESCRIBE FORMATTED** - Para cada uma das 12 tabelas
2. **SELECT * FROM tabela LIMIT 10** - Para obter dados de exemplo
3. Gera arquivos **Markdown** (.md) e **JSON** (.json) para cada tabela
4. Organiza em diretórios `originais/` e `intermediarias/`
5. Cria um **README.md** com índice de todas as tabelas

---

## 📋 Tabelas que serão processadas

### Tabelas Originais (6)
- `bcadastro_base_cnpj_completo`
- `bcadastro_base_socios_consolidado`
- `bcadastro_pgdas_consolidado`
- `bcadastro_tab_raiz_cpf_pai`
- `feitoza_base_periodos_sn`
- `feitoza_rba_12_meses`

### Tabelas Intermediárias (6)
- `bcadastro_output_final_acl`
- `feitoza_grupos_identificados`
- `feitoza_rba_grupo`
- `feitoza_fato_gerador`
- `feitoza_resumo_grupos_irregulares`
- `feitoza_lista_acao_fiscal`

---

## 🚀 Como executar

### Opção 1: Notebook Completo (Recomendado)

1. Abra `gerar_data_schemas.ipynb` no Jupyter
2. Execute todas as células em sequência (Run All)
3. Acompanhe o progresso com mensagens detalhadas
4. O notebook está organizado em seções:
   - Configuração Inicial
   - Funções Auxiliares
   - Processamento das Tabelas
   - Geração do Índice
   - Resumo Final

### Opção 2: Notebook Simplificado

1. Abra `gerar_schemas_simples.ipynb` no Jupyter
2. Execute todas as células em sequência
3. Versão mais direta, ideal para uso rápido
4. Menos células, mesmo resultado

**⚠️ Importante:** Os notebooks já incluem a correção para o problema de conflito entre `sum()` do Python e `pyspark.sql.functions.sum()` usando `builtins.sum()`.

---

## 📁 Estrutura de saída

Após a execução, será criada a seguinte estrutura:

```
BCadastro/
├── data-schemas/
│   ├── README.md                                    # Índice geral
│   ├── originais/
│   │   ├── bcadastro_base_cnpj_completo.md         # Schema em Markdown
│   │   ├── bcadastro_base_cnpj_completo.json       # Schema em JSON
│   │   ├── bcadastro_base_socios_consolidado.md
│   │   ├── bcadastro_base_socios_consolidado.json
│   │   └── ... (demais tabelas originais)
│   └── intermediarias/
│       ├── bcadastro_output_final_acl.md
│       ├── bcadastro_output_final_acl.json
│       ├── feitoza_grupos_identificados.md
│       ├── feitoza_grupos_identificados.json
│       └── ... (demais tabelas intermediárias)
└── gerar_data_schemas.py                           # Este script
```

---

## 📄 Conteúdo de cada arquivo

### Arquivo Markdown (.md)

Cada arquivo Markdown contém:

1. **Cabeçalho** - Nome da tabela, tipo, database, data de geração
2. **Estrutura da Tabela** - Lista de colunas com tipos e comentários
3. **Metadados** - Informações técnicas da tabela (location, storage, etc.)
4. **Dados de Exemplo** - Primeiras 10 linhas da tabela em formato de tabela
5. **Queries SQL** - Comandos para reproduzir os dados

### Arquivo JSON (.json)

Cada arquivo JSON contém:

```json
{
  "tabela": "nome_da_tabela",
  "database": "gessimples",
  "gerado_em": "2025-11-17T10:30:00",
  "describe_formatted": [ ... ],
  "sample_data": [ ... ]
}
```

---

## ⚙️ Configuração

### Requisitos

- **PySpark** instalado e configurado
- **Acesso ao banco de dados** `gessimples` (Impala/Hive)
- **Permissões de leitura** nas tabelas

### Personalização

Edite o arquivo `gerar_data_schemas.py` para:

1. **Alterar o database:**
   ```python
   DATABASE = "seu_database"  # Linha 12
   ```

2. **Adicionar/remover tabelas:**
   ```python
   TABELAS_ORIGINAIS = [
       "sua_tabela_1",
       "sua_tabela_2"
   ]
   ```

3. **Alterar quantidade de linhas de exemplo:**
   ```python
   query = f"SELECT * FROM {DATABASE}.{tabela} LIMIT 20"  # Linha 62
   ```

4. **Alterar diretório de saída:**
   ```python
   OUTPUT_DIR = "meu_diretorio"  # Linha 36
   ```

---

## 🐛 Troubleshooting

### Erro: "Table not found"

**Causa:** Tabela não existe no banco de dados

**Solução:**
- Verifique se a tabela existe: `SHOW TABLES IN gessimples;`
- Remova a tabela da lista no script

### Erro: "Permission denied"

**Causa:** Sem permissão para acessar a tabela

**Solução:**
- Verifique suas permissões no banco
- Contate o administrador do banco

### Erro: "Failed to connect"

**Causa:** Problemas de conexão com o banco

**Solução:**
- Verifique a configuração do Spark
- Teste a conexão: `spark.sql("SHOW DATABASES").show()`

### Script executa mas não gera arquivos

**Causa:** Erros nas queries SQL

**Solução:**
- Execute o script em modo debug
- Verifique os logs de erro no console
- Teste queries individuais no notebook

---

## 📊 Saída esperada

Ao executar o script, você verá algo como:

```
================================================================================
🚀 GERADOR DE DATA SCHEMAS - BCadastro
================================================================================
Database: gessimples
Total de tabelas: 12
Output: data-schemas/
================================================================================


📦 PROCESSANDO TABELAS ORIGINAIS
--------------------------------------------------------------------------------

================================================================================
📋 Processando: gessimples.bcadastro_base_cnpj_completo
================================================================================
Executando: DESCRIBE FORMATTED gessimples.bcadastro_base_cnpj_completo
Executando: SELECT * FROM gessimples.bcadastro_base_cnpj_completo LIMIT 10
✅ Markdown salvo em: data-schemas/originais/bcadastro_base_cnpj_completo.md
✅ JSON salvo em: data-schemas/originais/bcadastro_base_cnpj_completo.json

...

================================================================================
✨ PROCESSAMENTO CONCLUÍDO
================================================================================
✅ Sucesso: 12/12
❌ Falhas: 0/12
📁 Arquivos gerados: 24 (Markdown + JSON)
📂 Diretório de saída: data-schemas/
================================================================================
```

---

## 🔄 Próximos passos

Após gerar os data-schemas:

1. ✅ Revise os arquivos Markdown gerados
2. ✅ Verifique se todos os dados estão corretos
3. ✅ Adicione comentários/documentação adicional se necessário
4. ✅ Commit os arquivos no repositório:
   ```bash
   git add data-schemas/
   git commit -m "docs: adiciona data-schemas das tabelas"
   git push
   ```

---

## 📞 Suporte

Em caso de dúvidas ou problemas:

1. Verifique os logs de execução
2. Consulte a documentação do PySpark
3. Revise o notebook `BCADASTRO-Exemplo.ipynb` para exemplos de queries

---

**Script criado em:** 2025-11-17
**Versão:** 1.0
