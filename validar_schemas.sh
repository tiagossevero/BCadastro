#!/bin/bash

# =============================================================================
# Script de Validação - Data Schemas
# =============================================================================
# Verifica se todos os arquivos de data-schema foram gerados corretamente
# =============================================================================

echo "================================================================================"
echo "🔍 VALIDAÇÃO DE DATA SCHEMAS - BCadastro"
echo "================================================================================"
echo ""

# Configuração
DATA_SCHEMAS_DIR="data-schemas"
EXPECTED_ORIGINAIS=6
EXPECTED_INTERMEDIARIAS=6
EXPECTED_TOTAL=$((EXPECTED_ORIGINAIS + EXPECTED_INTERMEDIARIAS))

# Cores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Contadores
total_ok=0
total_erro=0

# =============================================================================
# VERIFICAR ESTRUTURA DE DIRETÓRIOS
# =============================================================================

echo "📁 Verificando estrutura de diretórios..."
echo "--------------------------------------------------------------------------------"

if [ ! -d "$DATA_SCHEMAS_DIR" ]; then
    echo -e "${RED}❌ Diretório $DATA_SCHEMAS_DIR não encontrado!${NC}"
    echo "   Execute primeiro: spark-submit gerar_data_schemas.py"
    exit 1
else
    echo -e "${GREEN}✅ Diretório $DATA_SCHEMAS_DIR encontrado${NC}"
fi

if [ ! -d "$DATA_SCHEMAS_DIR/originais" ]; then
    echo -e "${RED}❌ Diretório $DATA_SCHEMAS_DIR/originais não encontrado!${NC}"
    ((total_erro++))
else
    echo -e "${GREEN}✅ Diretório originais/ encontrado${NC}"
    ((total_ok++))
fi

if [ ! -d "$DATA_SCHEMAS_DIR/intermediarias" ]; then
    echo -e "${RED}❌ Diretório $DATA_SCHEMAS_DIR/intermediarias não encontrado!${NC}"
    ((total_erro++))
else
    echo -e "${GREEN}✅ Diretório intermediarias/ encontrado${NC}"
    ((total_ok++))
fi

echo ""

# =============================================================================
# VERIFICAR ARQUIVOS MARKDOWN
# =============================================================================

echo "📝 Verificando arquivos Markdown (.md)..."
echo "--------------------------------------------------------------------------------"

# Contar arquivos MD
md_originais=$(find "$DATA_SCHEMAS_DIR/originais" -name "*.md" 2>/dev/null | wc -l)
md_intermediarias=$(find "$DATA_SCHEMAS_DIR/intermediarias" -name "*.md" 2>/dev/null | wc -l)
md_total=$((md_originais + md_intermediarias))

echo "Tabelas Originais: $md_originais/$EXPECTED_ORIGINAIS"
echo "Tabelas Intermediárias: $md_intermediarias/$EXPECTED_INTERMEDIARIAS"
echo "Total: $md_total/$EXPECTED_TOTAL"

if [ $md_total -eq $EXPECTED_TOTAL ]; then
    echo -e "${GREEN}✅ Todos os arquivos Markdown foram gerados!${NC}"
    ((total_ok++))
else
    echo -e "${RED}❌ Faltam $(($EXPECTED_TOTAL - $md_total)) arquivos Markdown${NC}"
    ((total_erro++))
fi

echo ""

# =============================================================================
# VERIFICAR ARQUIVOS JSON
# =============================================================================

echo "📦 Verificando arquivos JSON (.json)..."
echo "--------------------------------------------------------------------------------"

# Contar arquivos JSON
json_originais=$(find "$DATA_SCHEMAS_DIR/originais" -name "*.json" 2>/dev/null | wc -l)
json_intermediarias=$(find "$DATA_SCHEMAS_DIR/intermediarias" -name "*.json" 2>/dev/null | wc -l)
json_total=$((json_originais + json_intermediarias))

echo "Tabelas Originais: $json_originais/$EXPECTED_ORIGINAIS"
echo "Tabelas Intermediárias: $json_intermediarias/$EXPECTED_INTERMEDIARIAS"
echo "Total: $json_total/$EXPECTED_TOTAL"

if [ $json_total -eq $EXPECTED_TOTAL ]; then
    echo -e "${GREEN}✅ Todos os arquivos JSON foram gerados!${NC}"
    ((total_ok++))
else
    echo -e "${RED}❌ Faltam $(($EXPECTED_TOTAL - $json_total)) arquivos JSON${NC}"
    ((total_erro++))
fi

echo ""

# =============================================================================
# VERIFICAR README
# =============================================================================

echo "📄 Verificando README.md..."
echo "--------------------------------------------------------------------------------"

if [ -f "$DATA_SCHEMAS_DIR/README.md" ]; then
    readme_size=$(stat -f%z "$DATA_SCHEMAS_DIR/README.md" 2>/dev/null || stat -c%s "$DATA_SCHEMAS_DIR/README.md" 2>/dev/null)
    if [ "$readme_size" -gt 100 ]; then
        echo -e "${GREEN}✅ README.md encontrado (${readme_size} bytes)${NC}"
        ((total_ok++))
    else
        echo -e "${YELLOW}⚠️  README.md muito pequeno (${readme_size} bytes)${NC}"
        ((total_erro++))
    fi
else
    echo -e "${RED}❌ README.md não encontrado${NC}"
    ((total_erro++))
fi

echo ""

# =============================================================================
# VERIFICAR CONTEÚDO DOS ARQUIVOS
# =============================================================================

echo "🔎 Verificando conteúdo dos arquivos..."
echo "--------------------------------------------------------------------------------"

# Lista de tabelas esperadas
declare -a tabelas_originais=(
    "bcadastro_base_cnpj_completo"
    "bcadastro_base_socios_consolidado"
    "bcadastro_pgdas_consolidado"
    "bcadastro_tab_raiz_cpf_pai"
    "feitoza_base_periodos_sn"
    "feitoza_rba_12_meses"
)

declare -a tabelas_intermediarias=(
    "bcadastro_output_final_acl"
    "feitoza_grupos_identificados"
    "feitoza_rba_grupo"
    "feitoza_fato_gerador"
    "feitoza_resumo_grupos_irregulares"
    "feitoza_lista_acao_fiscal"
)

# Verificar tabelas originais
echo "Verificando tabelas originais:"
for tabela in "${tabelas_originais[@]}"; do
    md_file="$DATA_SCHEMAS_DIR/originais/${tabela}.md"
    json_file="$DATA_SCHEMAS_DIR/originais/${tabela}.json"

    if [ -f "$md_file" ] && [ -f "$json_file" ]; then
        # Verificar se MD não está vazio
        if [ -s "$md_file" ]; then
            echo -e "  ${GREEN}✅${NC} $tabela"
        else
            echo -e "  ${RED}❌${NC} $tabela (arquivo vazio)"
        fi
    else
        echo -e "  ${RED}❌${NC} $tabela (arquivos faltando)"
    fi
done

# Verificar tabelas intermediárias
echo ""
echo "Verificando tabelas intermediárias:"
for tabela in "${tabelas_intermediarias[@]}"; do
    md_file="$DATA_SCHEMAS_DIR/intermediarias/${tabela}.md"
    json_file="$DATA_SCHEMAS_DIR/intermediarias/${tabela}.json"

    if [ -f "$md_file" ] && [ -f "$json_file" ]; then
        # Verificar se MD não está vazio
        if [ -s "$md_file" ]; then
            echo -e "  ${GREEN}✅${NC} $tabela"
        else
            echo -e "  ${RED}❌${NC} $tabela (arquivo vazio)"
        fi
    else
        echo -e "  ${RED}❌${NC} $tabela (arquivos faltando)"
    fi
done

echo ""

# =============================================================================
# ESTATÍSTICAS DE TAMANHO
# =============================================================================

echo "📊 Estatísticas de tamanho..."
echo "--------------------------------------------------------------------------------"

# Tamanho total
total_size=$(du -sh "$DATA_SCHEMAS_DIR" 2>/dev/null | cut -f1)
echo "Tamanho total: $total_size"

# Tamanho por tipo
originais_size=$(du -sh "$DATA_SCHEMAS_DIR/originais" 2>/dev/null | cut -f1)
intermediarias_size=$(du -sh "$DATA_SCHEMAS_DIR/intermediarias" 2>/dev/null | cut -f1)
echo "  - Originais: $originais_size"
echo "  - Intermediárias: $intermediarias_size"

# Contar total de arquivos
total_files=$(find "$DATA_SCHEMAS_DIR" -type f | wc -l)
echo "Total de arquivos: $total_files"

echo ""

# =============================================================================
# RESUMO FINAL
# =============================================================================

echo "================================================================================"
echo "📋 RESUMO DA VALIDAÇÃO"
echo "================================================================================"
echo ""

expected_files=$((EXPECTED_TOTAL * 2 + 1))  # MD + JSON + README

echo "Arquivos esperados: $expected_files"
echo "Arquivos encontrados: $total_files"
echo ""

if [ $total_erro -eq 0 ] && [ $total_files -eq $expected_files ]; then
    echo -e "${GREEN}✅ VALIDAÇÃO COMPLETA - Todos os data-schemas foram gerados com sucesso!${NC}"
    echo ""
    echo "Próximos passos:"
    echo "  1. Revisar os arquivos em: $DATA_SCHEMAS_DIR/"
    echo "  2. git add $DATA_SCHEMAS_DIR/"
    echo "  3. git commit -m 'docs: adiciona data-schemas das tabelas'"
    echo "  4. git push"
    exit 0
else
    echo -e "${RED}❌ VALIDAÇÃO FALHOU - Alguns arquivos estão faltando ou com problemas${NC}"
    echo ""
    echo "Ações recomendadas:"
    echo "  1. Verifique os erros acima"
    echo "  2. Re-execute: spark-submit gerar_data_schemas.py"
    echo "  3. Verifique logs de erro"
    exit 1
fi

echo "================================================================================"
