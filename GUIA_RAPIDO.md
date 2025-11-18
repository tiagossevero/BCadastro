# 🚀 Guia Rápido - Gerador de Data Schemas

## 📝 Resumo

Gerar automaticamente **DESCRIBE FORMATTED** e **SELECT LIMIT 10** para **12 tabelas** do BCadastro.

---

## ⚡ Execução Rápida

### Opção 1: Script Python completo (Recomendado)

```bash
spark-submit gerar_data_schemas.py
```

### Opção 2: No Jupyter Notebook

Adicione uma nova célula no `BCADASTRO-Exemplo.ipynb`:

```python
%run gerar_schemas_notebook.py
```

### Opção 3: Diretamente no PySpark shell

```python
exec(open('gerar_schemas_notebook.py').read())
```

---

## 📋 Tabelas (12 total)

### ✅ 6 Originais

| # | Tabela | Descrição |
|---|--------|-----------|
| 1 | `bcadastro_base_cnpj_completo` | Base cadastral de empresas |
| 2 | `bcadastro_base_socios_consolidado` | Base de sócios |
| 3 | `bcadastro_pgdas_consolidado` | Declarações PGDAS |
| 4 | `bcadastro_tab_raiz_cpf_pai` | Histórico RBA por CPF |
| 5 | `feitoza_base_periodos_sn` | Períodos no Simples Nacional |
| 6 | `feitoza_rba_12_meses` | Receita Bruta Acumulada 12m |

### ✅ 6 Intermediárias

| # | Tabela | Descrição |
|---|--------|-----------|
| 7 | `bcadastro_output_final_acl` | **Output principal** - Grupos irregulares |
| 8 | `feitoza_grupos_identificados` | Grupos econômicos identificados |
| 9 | `feitoza_rba_grupo` | RBA consolidada por grupo |
| 10 | `feitoza_fato_gerador` | Fatos geradores de exclusão |
| 11 | `feitoza_resumo_grupos_irregulares` | Resumo grupos irregulares |
| 12 | `feitoza_lista_acao_fiscal` | Lista priorizada para fiscalização |

---

## 📁 Output Gerado

```
data-schemas/
├── README.md                                    # Índice geral
├── originais/
│   ├── bcadastro_base_cnpj_completo.md         # 6 arquivos .md
│   ├── bcadastro_base_cnpj_completo.json       # 6 arquivos .json
│   └── ... (outras 5 tabelas)
└── intermediarias/
    ├── bcadastro_output_final_acl.md           # 6 arquivos .md
    ├── bcadastro_output_final_acl.json         # 6 arquivos .json
    └── ... (outras 5 tabelas)

Total: 25 arquivos (1 README + 12 MD + 12 JSON)
```

---

## 🔍 Testar Manualmente

### Query individual (PySpark):

```python
# DESCRIBE FORMATTED
spark.sql("DESCRIBE FORMATTED gessimples.bcadastro_base_cnpj_completo").show(100, truncate=False)

# SELECT SAMPLE
spark.sql("SELECT * FROM gessimples.bcadastro_base_cnpj_completo LIMIT 10").show(truncate=False)
```

### Todas as queries de exemplo:

```bash
# Ver arquivo com todas as queries
cat queries_exemplo.sql
```

---

## 📊 Conteúdo dos Arquivos

Cada arquivo `.md` contém:

1. ✅ Cabeçalho (nome, tipo, database, data)
2. ✅ Estrutura da tabela (colunas + tipos)
3. ✅ Metadados (location, storage, etc.)
4. ✅ Dados de exemplo (10 linhas em tabela)
5. ✅ Queries SQL de referência

Cada arquivo `.json` contém:

- Dados estruturados do DESCRIBE FORMATTED
- Dados de exemplo (10 linhas)
- Metadados da geração

---

## 🛠️ Personalização Rápida

### Alterar quantidade de linhas:

Edite o script e modifique:

```python
LIMIT 10  →  LIMIT 20
```

### Adicionar/remover tabelas:

Edite as listas:

```python
TABELAS_ORIGINAIS = [
    "sua_tabela_aqui",
    ...
]
```

### Alterar database:

```python
DATABASE = "gessimples"  →  DATABASE = "seu_db"
```

---

## 🐛 Problemas Comuns

| Erro | Solução |
|------|---------|
| `Table not found` | Verifique se a tabela existe: `SHOW TABLES IN gessimples;` |
| `Permission denied` | Verifique permissões no banco |
| `Connection failed` | Teste conexão: `spark.sql("SHOW DATABASES").show()` |
| Arquivos não gerados | Verifique logs de erro no console |

---

## ✅ Checklist

- [ ] PySpark instalado e configurado
- [ ] Acesso ao banco `gessimples`
- [ ] Permissões de leitura nas tabelas
- [ ] Script executado com sucesso
- [ ] 25 arquivos gerados (1 README + 12 MD + 12 JSON)
- [ ] Dados de exemplo aparecem corretamente
- [ ] Commit dos arquivos no git

---

## 📚 Arquivos Criados

| Arquivo | Descrição |
|---------|-----------|
| `gerar_data_schemas.py` | **Script principal** - Completo com JSON e MD |
| `gerar_schemas_notebook.py` | **Versão notebook** - Simplificada para Jupyter |
| `queries_exemplo.sql` | Queries individuais para teste manual |
| `INSTRUCOES_DATA_SCHEMAS.md` | Documentação completa |
| `GUIA_RAPIDO.md` | Este guia (referência rápida) |

---

## 🎯 Próximos Passos

1. **Execute o script:**
   ```bash
   spark-submit gerar_data_schemas.py
   ```

2. **Revise os outputs:**
   ```bash
   ls -lh data-schemas/
   cat data-schemas/README.md
   ```

3. **Commit no git:**
   ```bash
   git add data-schemas/
   git commit -m "docs: adiciona data-schemas das tabelas"
   git push
   ```

---

## 💡 Dicas

- 🚀 Execute em horário de baixo uso do cluster
- 📊 Verifique se todas as 12 tabelas foram processadas
- 🔍 Revise os dados de exemplo para validar
- 📝 Adicione comentários adicionais nos arquivos MD se necessário
- 🔄 Re-execute periodicamente se os schemas mudarem

---

**Criado em:** 2025-11-17
**Database:** `gessimples` (Apache Impala)
**Total de tabelas:** 12 (6 originais + 6 intermediárias)
