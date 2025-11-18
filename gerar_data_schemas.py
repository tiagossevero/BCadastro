"""
Script para gerar automaticamente os data-schemas de todas as tabelas do projeto BCadastro
Executa DESCRIBE FORMATTED e SELECT * LIMIT 10 para cada tabela
"""

from pyspark.sql import SparkSession
import json
import os
from datetime import datetime

# Inicializar Spark Session
spark = SparkSession.builder \
    .appName("BCadastro - Gerador de Data Schemas") \
    .enableHiveSupport() \
    .getOrCreate()

# Definir database
DATABASE = "gessimples"

# Lista de tabelas a serem documentadas
TABELAS_ORIGINAIS = [
    "bcadastro_base_cnpj_completo",
    "bcadastro_base_socios_consolidado",
    "bcadastro_pgdas_consolidado",
    "bcadastro_tab_raiz_cpf_pai",
    "feitoza_base_periodos_sn",
    "feitoza_rba_12_meses"
]

TABELAS_INTERMEDIARIAS = [
    "bcadastro_output_final_acl",
    "feitoza_grupos_identificados",
    "feitoza_rba_grupo",
    "feitoza_fato_gerador",
    "feitoza_resumo_grupos_irregulares",
    "feitoza_lista_acao_fiscal"
]

# Diretório de saída
OUTPUT_DIR = "data-schemas"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/originais", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/intermediarias", exist_ok=True)


def executar_describe_formatted(tabela):
    """Executa DESCRIBE FORMATTED e retorna os resultados"""
    try:
        query = f"DESCRIBE FORMATTED {DATABASE}.{tabela}"
        print(f"Executando: {query}")
        df = spark.sql(query)
        resultado = df.collect()
        return [row.asDict() for row in resultado]
    except Exception as e:
        print(f"❌ Erro ao executar DESCRIBE FORMATTED em {tabela}: {str(e)}")
        return None


def executar_select_sample(tabela):
    """Executa SELECT * LIMIT 10 e retorna os resultados"""
    try:
        query = f"SELECT * FROM {DATABASE}.{tabela} LIMIT 10"
        print(f"Executando: {query}")
        df = spark.sql(query)
        resultado = df.collect()

        # Converter para dicionário, tratando tipos especiais
        data = []
        for row in resultado:
            row_dict = row.asDict()
            # Converter valores não serializáveis para string
            for key, value in row_dict.items():
                if value is not None and not isinstance(value, (str, int, float, bool, list, dict)):
                    row_dict[key] = str(value)
            data.append(row_dict)

        return data
    except Exception as e:
        print(f"❌ Erro ao executar SELECT em {tabela}: {str(e)}")
        return None


def gerar_schema_markdown(tabela, tipo_tabela, describe_result, sample_data):
    """Gera arquivo Markdown com o schema da tabela"""

    # Extrair informações do DESCRIBE FORMATTED
    colunas = []
    metadata = {}
    secao_atual = None

    for row in describe_result:
        col_name = row.get('col_name', '').strip()
        data_type = row.get('data_type', '').strip()
        comment = row.get('comment', '') or ''

        # Detectar seções
        if col_name.startswith('#'):
            secao_atual = col_name
            continue

        # Colunas da tabela
        if col_name and data_type and secao_atual != '# Detailed Table Information':
            if col_name not in ['', '# col_name']:
                colunas.append({
                    'nome': col_name,
                    'tipo': data_type,
                    'comentario': comment
                })

        # Metadata da tabela
        if secao_atual == '# Detailed Table Information' and col_name and data_type:
            metadata[col_name] = data_type

    # Gerar conteúdo Markdown
    md_content = f"""# Data Schema: {tabela}

**Tipo:** {tipo_tabela}
**Database:** {DATABASE}
**Gerado em:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## 📋 Estrutura da Tabela

### Colunas

| Nome da Coluna | Tipo de Dado | Comentário |
|---------------|-------------|-----------|
"""

    for col in colunas:
        comentario = col['comentario'] if col['comentario'] else '-'
        md_content += f"| `{col['nome']}` | `{col['tipo']}` | {comentario} |\n"

    # Adicionar metadata
    md_content += "\n\n### 🔧 Metadados da Tabela\n\n"
    md_content += "| Propriedade | Valor |\n"
    md_content += "|------------|-------|\n"

    for key, value in metadata.items():
        if key and value:
            md_content += f"| {key} | {value} |\n"

    # Adicionar dados de exemplo
    md_content += "\n\n---\n\n## 📊 Dados de Exemplo (LIMIT 10)\n\n"

    if sample_data and len(sample_data) > 0:
        # Criar tabela markdown com os dados
        if len(sample_data) > 0:
            # Cabeçalho
            headers = list(sample_data[0].keys())
            md_content += "| " + " | ".join(headers) + " |\n"
            md_content += "|" + "|".join(["---" for _ in headers]) + "|\n"

            # Linhas de dados
            for row in sample_data:
                values = []
                for header in headers:
                    value = row.get(header, '')
                    # Truncar valores muito longos
                    if value is not None:
                        value_str = str(value)
                        if len(value_str) > 50:
                            value_str = value_str[:47] + "..."
                        values.append(value_str)
                    else:
                        values.append("NULL")
                md_content += "| " + " | ".join(values) + " |\n"
    else:
        md_content += "*Nenhum dado disponível*\n"

    # Adicionar query SQL
    md_content += f"\n\n---\n\n## 🔍 Queries de Referência\n\n"
    md_content += f"### Describe Formatted\n\n```sql\nDESCRIBE FORMATTED {DATABASE}.{tabela};\n```\n\n"
    md_content += f"### Select Sample\n\n```sql\nSELECT * FROM {DATABASE}.{tabela} LIMIT 10;\n```\n"

    return md_content


def gerar_schema_json(tabela, describe_result, sample_data):
    """Gera arquivo JSON com o schema da tabela"""
    schema_json = {
        "tabela": tabela,
        "database": DATABASE,
        "gerado_em": datetime.now().isoformat(),
        "describe_formatted": describe_result,
        "sample_data": sample_data
    }
    return json.dumps(schema_json, indent=2, ensure_ascii=False)


def processar_tabela(tabela, tipo_tabela):
    """Processa uma tabela e gera os arquivos de schema"""
    print(f"\n{'='*80}")
    print(f"📋 Processando: {DATABASE}.{tabela}")
    print(f"{'='*80}")

    # Executar queries
    describe_result = executar_describe_formatted(tabela)
    sample_data = executar_select_sample(tabela)

    if describe_result is None:
        print(f"⚠️  Pulando {tabela} - Erro no DESCRIBE FORMATTED")
        return False

    # Determinar diretório de saída
    subdir = "originais" if tipo_tabela == "Original" else "intermediarias"

    # Gerar e salvar Markdown
    md_content = gerar_schema_markdown(tabela, tipo_tabela, describe_result, sample_data)
    md_file = f"{OUTPUT_DIR}/{subdir}/{tabela}.md"
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write(md_content)
    print(f"✅ Markdown salvo em: {md_file}")

    # Gerar e salvar JSON
    json_content = gerar_schema_json(tabela, describe_result, sample_data)
    json_file = f"{OUTPUT_DIR}/{subdir}/{tabela}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        f.write(json_content)
    print(f"✅ JSON salvo em: {json_file}")

    return True


def main():
    """Função principal"""
    print("="*80)
    print("🚀 GERADOR DE DATA SCHEMAS - BCadastro")
    print("="*80)
    print(f"Database: {DATABASE}")
    print(f"Total de tabelas: {len(TABELAS_ORIGINAIS) + len(TABELAS_INTERMEDIARIAS)}")
    print(f"Output: {OUTPUT_DIR}/")
    print("="*80)

    sucesso = 0
    falha = 0

    # Processar tabelas originais
    print("\n\n📦 PROCESSANDO TABELAS ORIGINAIS")
    print("-"*80)
    for tabela in TABELAS_ORIGINAIS:
        if processar_tabela(tabela, "Original"):
            sucesso += 1
        else:
            falha += 1

    # Processar tabelas intermediárias
    print("\n\n🔄 PROCESSANDO TABELAS INTERMEDIÁRIAS")
    print("-"*80)
    for tabela in TABELAS_INTERMEDIARIAS:
        if processar_tabela(tabela, "Intermediária"):
            sucesso += 1
        else:
            falha += 1

    # Gerar índice geral
    print("\n\n📑 GERANDO ÍNDICE GERAL")
    print("-"*80)

    index_content = f"""# Data Schemas - BCadastro

**Database:** {DATABASE}
**Gerado em:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Total de tabelas:** {len(TABELAS_ORIGINAIS) + len(TABELAS_INTERMEDIARIAS)}

---

## 📦 Tabelas Originais ({len(TABELAS_ORIGINAIS)})

Tabelas fonte de dados do banco de dados:

"""

    for tabela in TABELAS_ORIGINAIS:
        index_content += f"- [{tabela}](originais/{tabela}.md)\n"

    index_content += f"\n\n## 🔄 Tabelas Intermediárias ({len(TABELAS_INTERMEDIARIAS)})\n\n"
    index_content += "Tabelas criadas pelo processo de ETL:\n\n"

    for tabela in TABELAS_INTERMEDIARIAS:
        index_content += f"- [{tabela}](intermediarias/{tabela}.md)\n"

    index_content += f"""

---

## 📊 Estatísticas

- ✅ Processadas com sucesso: {sucesso}
- ❌ Falhas: {falha}
- 📁 Total de arquivos gerados: {sucesso * 2} (Markdown + JSON)

---

## 🔍 Estrutura de Diretórios

```
data-schemas/
├── README.md (este arquivo)
├── originais/
│   ├── *.md (schemas em Markdown)
│   └── *.json (schemas em JSON)
└── intermediarias/
    ├── *.md (schemas em Markdown)
    └── *.json (schemas em JSON)
```

---

**Gerado automaticamente por:** `gerar_data_schemas.py`
"""

    readme_file = f"{OUTPUT_DIR}/README.md"
    with open(readme_file, 'w', encoding='utf-8') as f:
        f.write(index_content)
    print(f"✅ Índice salvo em: {readme_file}")

    # Resumo final
    print("\n\n" + "="*80)
    print("✨ PROCESSAMENTO CONCLUÍDO")
    print("="*80)
    print(f"✅ Sucesso: {sucesso}/{len(TABELAS_ORIGINAIS) + len(TABELAS_INTERMEDIARIAS)}")
    print(f"❌ Falhas: {falha}/{len(TABELAS_ORIGINAIS) + len(TABELAS_INTERMEDIARIAS)}")
    print(f"📁 Arquivos gerados: {sucesso * 2} (Markdown + JSON)")
    print(f"📂 Diretório de saída: {OUTPUT_DIR}/")
    print("="*80)


if __name__ == "__main__":
    main()
    spark.stop()
