"""
Versão simplificada para executar diretamente no Jupyter Notebook
Copie e cole este código em uma nova célula no BCADASTRO-Exemplo.ipynb
"""

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================

DATABASE = "gessimples"
OUTPUT_DIR = "data-schemas"

# Lista de tabelas
TABELAS = {
    "originais": [
        "bcadastro_base_cnpj_completo",
        "bcadastro_base_socios_consolidado",
        "bcadastro_pgdas_consolidado",
        "bcadastro_tab_raiz_cpf_pai",
        "feitoza_base_periodos_sn",
        "feitoza_rba_12_meses"
    ],
    "intermediarias": [
        "bcadastro_output_final_acl",
        "feitoza_grupos_identificados",
        "feitoza_rba_grupo",
        "feitoza_fato_gerador",
        "feitoza_resumo_grupos_irregulares",
        "feitoza_lista_acao_fiscal"
    ]
}

# =============================================================================
# FUNÇÕES
# =============================================================================

import os
from datetime import datetime

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/originais", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/intermediarias", exist_ok=True)

def gerar_schema(tabela, tipo):
    """Gera o data-schema de uma tabela"""
    print(f"\n{'='*80}")
    print(f"📋 Processando: {DATABASE}.{tabela}")
    print(f"{'='*80}")

    try:
        # DESCRIBE FORMATTED
        print(f"▶ Executando DESCRIBE FORMATTED...")
        describe_df = spark.sql(f"DESCRIBE FORMATTED {DATABASE}.{tabela}")
        describe_data = describe_df.collect()

        # SELECT SAMPLE
        print(f"▶ Executando SELECT... LIMIT 10")
        sample_df = spark.sql(f"SELECT * FROM {DATABASE}.{tabela} LIMIT 10")
        sample_data = sample_df.collect()

        # Processar DESCRIBE FORMATTED
        colunas = []
        metadata = {}
        secao_atual = None

        for row in describe_data:
            col_name = row['col_name'].strip() if row['col_name'] else ''
            data_type = row['data_type'].strip() if row['data_type'] else ''
            comment = row['comment'] if row['comment'] else ''

            # Detectar seções
            if col_name.startswith('#'):
                secao_atual = col_name
                continue

            # Colunas
            if col_name and data_type and secao_atual != '# Detailed Table Information':
                if col_name not in ['', '# col_name']:
                    colunas.append({
                        'nome': col_name,
                        'tipo': data_type,
                        'comentario': comment
                    })

            # Metadata
            if secao_atual == '# Detailed Table Information' and col_name and data_type:
                metadata[col_name] = data_type

        # Gerar Markdown
        subdir = "originais" if tipo == "Original" else "intermediarias"
        md_file = f"{OUTPUT_DIR}/{subdir}/{tabela}.md"

        with open(md_file, 'w', encoding='utf-8') as f:
            # Cabeçalho
            f.write(f"# Data Schema: {tabela}\n\n")
            f.write(f"**Tipo:** {tipo}\n")
            f.write(f"**Database:** {DATABASE}\n")
            f.write(f"**Gerado em:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("---\n\n")

            # Estrutura da Tabela
            f.write("## 📋 Estrutura da Tabela\n\n")
            f.write("### Colunas\n\n")
            f.write("| Nome da Coluna | Tipo de Dado | Comentário |\n")
            f.write("|---------------|-------------|------------|\n")

            for col in colunas:
                comentario = col['comentario'] if col['comentario'] else '-'
                f.write(f"| `{col['nome']}` | `{col['tipo']}` | {comentario} |\n")

            # Metadados
            f.write("\n\n### 🔧 Metadados da Tabela\n\n")
            f.write("| Propriedade | Valor |\n")
            f.write("|------------|-------|\n")

            for key, value in metadata.items():
                if key and value:
                    f.write(f"| {key} | {value} |\n")

            # Dados de exemplo
            f.write("\n\n---\n\n")
            f.write("## 📊 Dados de Exemplo (LIMIT 10)\n\n")

            if sample_data and len(sample_data) > 0:
                # Cabeçalho
                headers = list(sample_data[0].asDict().keys())
                f.write("| " + " | ".join(headers) + " |\n")
                f.write("|" + "|".join(["---" for _ in headers]) + "|\n")

                # Linhas
                for row in sample_data:
                    row_dict = row.asDict()
                    values = []
                    for header in headers:
                        value = row_dict.get(header, '')
                        if value is not None:
                            value_str = str(value)
                            if len(value_str) > 50:
                                value_str = value_str[:47] + "..."
                            values.append(value_str)
                        else:
                            values.append("NULL")
                    f.write("| " + " | ".join(values) + " |\n")
            else:
                f.write("*Nenhum dado disponível*\n")

            # Queries
            f.write("\n\n---\n\n")
            f.write("## 🔍 Queries de Referência\n\n")
            f.write("### Describe Formatted\n\n")
            f.write(f"```sql\nDESCRIBE FORMATTED {DATABASE}.{tabela};\n```\n\n")
            f.write("### Select Sample\n\n")
            f.write(f"```sql\nSELECT * FROM {DATABASE}.{tabela} LIMIT 10;\n```\n")

        print(f"✅ Schema gerado: {md_file}")
        return True

    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        return False

# =============================================================================
# EXECUÇÃO
# =============================================================================

print("="*80)
print("🚀 GERADOR DE DATA SCHEMAS - BCadastro")
print("="*80)
print(f"Database: {DATABASE}")
print(f"Output: {OUTPUT_DIR}/")
print("="*80)

sucesso = 0
falha = 0

# Processar tabelas originais
print("\n\n📦 PROCESSANDO TABELAS ORIGINAIS")
print("-"*80)
for tabela in TABELAS["originais"]:
    if gerar_schema(tabela, "Original"):
        sucesso += 1
    else:
        falha += 1

# Processar tabelas intermediárias
print("\n\n🔄 PROCESSANDO TABELAS INTERMEDIÁRIAS")
print("-"*80)
for tabela in TABELAS["intermediarias"]:
    if gerar_schema(tabela, "Intermediária"):
        sucesso += 1
    else:
        falha += 1

# Gerar README
print("\n\n📑 GERANDO README")
print("-"*80)

readme_content = f"""# Data Schemas - BCadastro

**Database:** {DATABASE}
**Gerado em:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Total de tabelas:** {len(TABELAS['originais']) + len(TABELAS['intermediarias'])}

---

## 📦 Tabelas Originais ({len(TABELAS['originais'])})

"""

for tabela in TABELAS['originais']:
    readme_content += f"- [{tabela}](originais/{tabela}.md)\n"

readme_content += f"\n\n## 🔄 Tabelas Intermediárias ({len(TABELAS['intermediarias'])})\n\n"

for tabela in TABELAS['intermediarias']:
    readme_content += f"- [{tabela}](intermediarias/{tabela}.md)\n"

readme_content += f"""

---

## 📊 Estatísticas

- ✅ Processadas com sucesso: {sucesso}
- ❌ Falhas: {falha}

---

**Gerado por:** `gerar_schemas_notebook.py`
"""

with open(f"{OUTPUT_DIR}/README.md", 'w', encoding='utf-8') as f:
    f.write(readme_content)

print(f"✅ README gerado: {OUTPUT_DIR}/README.md")

# Resumo final
print("\n\n" + "="*80)
print("✨ PROCESSAMENTO CONCLUÍDO")
print("="*80)
print(f"✅ Sucesso: {sucesso}/{len(TABELAS['originais']) + len(TABELAS['intermediarias'])}")
print(f"❌ Falhas: {falha}/{len(TABELAS['originais']) + len(TABELAS['intermediarias'])}")
print(f"📂 Diretório de saída: {OUTPUT_DIR}/")
print("="*80)
