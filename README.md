# 🏢 GENESIS - Sistema de Análise de Grupos Econômicos

> Dashboard interativo para análise e fiscalização de grupos econômicos no Simples Nacional
> Receita Estadual de Santa Catarina - Versão 2.0

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-2.0+-red.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

---

## 📋 Sobre o Projeto

O **Sistema GENESIS** (Grupos Econômicos e Simples Nacional) é uma aplicação de Business Intelligence desenvolvida em Python/Streamlit para a Receita Estadual de Santa Catarina. O sistema identifica e analisa grupos econômicos que violam os critérios da **Lei Complementar 123/2006, Art. 3º, § 4º, Inciso IV**.

### 🎯 Objetivo Principal

Identificar empresas do Simples Nacional irregulares quando:
- Um sócio participa com mais de 10% em outra empresa não beneficiada pelo Simples
- A receita bruta global do grupo ultrapassa R$ 4.800.000,00

O sistema realiza **fiscalização preventiva** e gera automaticamente Termos de Exclusão (TE) para empresas em situação irregular.

---

## ✨ Funcionalidades

### 📊 Dashboard Executivo
- **KPIs em tempo real**: Grupos, Empresas, Sócios, Crédito Tributário, Receita
- **Distribuições visuais**: Ação Fiscal, Períodos, Estados, Qualificações
- **Alertas categorizados**: Exclusão com/sem débito, sem interesse

### 🏆 Ranking de Grupos
- Top 100 grupos por crédito tributário
- Filtros interativos por ação fiscal e período
- Exportação de dados

### 🔬 Análise Detalhada - Grupo
- Busca por número de grupo ou CPF do sócio
- Detalhamento completo de empresas e sócios
- Distribuição geográfica e temporal
- Gráficos de evolução e relacionamentos

### 🔍 Análise Detalhada - Empresa
- Busca por razão social ou CNPJ
- Dados cadastrais completos
- Informações de matriz/filial
- CNAE, porte, natureza jurídica

### 📄 Relatório Executivo
- Sumário consolidado com contexto legal
- Top 50 grupos prioritários para fiscalização
- Recomendações categorizadas por ação

### 📋 Base Cadastral
- Estatísticas gerais da base de dados
- Distribuição por porte e natureza jurídica
- Métricas de capital social

---

## 🛠️ Tecnologias Utilizadas

### Core
- **Python 3.8+** - Linguagem principal
- **Streamlit 2.0+** - Framework web interativo
- **Pandas** - Manipulação de dados
- **NumPy** - Operações numéricas

### Visualização
- **Plotly Express** - Gráficos interativos
- **Plotly Graph Objects** - Componentes avançados

### Banco de Dados
- **SQLAlchemy** - ORM para conexão com BD
- **Apache Impala** - Data Warehouse

### Utilitários
- **SSL** - Certificados e segurança
- **Datetime** - Manipulação de datas
- **Warnings** - Controle de avisos

---

## 📦 Pré-requisitos

### Sistema
- Python 3.8 ou superior
- pip (gerenciador de pacotes Python)
- Acesso ao banco de dados Apache Impala

### Bibliotecas Python
```bash
streamlit>=2.0.0
pandas>=1.3.0
numpy>=1.21.0
plotly>=5.0.0
sqlalchemy>=1.4.0
impyla>=0.17.0
```

---

## 🚀 Instalação

### 1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/BCadastro.git
cd BCadastro
```

### 2. Crie um ambiente virtual (recomendado)
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 3. Instale as dependências
```bash
pip install -r requirements.txt
```

### 4. Configure as credenciais
Crie o arquivo `.streamlit/secrets.toml` com as credenciais do Impala:

```toml
[impala_credentials]
user = "seu_usuario"
password = "sua_senha"
```

### 5. Configure a senha de acesso (opcional)
No arquivo `BCADASTRO.py`, linha 45, altere a senha padrão:

```python
SENHA = "sua_senha_personalizada"
```

---

## ⚙️ Configuração

### Conexão com Banco de Dados

O sistema conecta-se ao Apache Impala com as seguintes configurações:

```
Host: bdaworkernode02.sef.sc.gov.br
Porta: 21050
Database: gessimples
Autenticação: LDAP
SSL: Habilitado
```

### Cache e Performance

O sistema utiliza cache otimizado:
- **Dados agregados**: TTL de 1 hora
- **Detalhes sob demanda**: TTL de 30 minutos
- **Engine Impala**: Recurso compartilhado

### Tabelas Utilizadas

| Tabela | Descrição |
|--------|-----------|
| `bcadastro_output_final_acl` | Grupos irregulares identificados |
| `bcadastro_base_cnpj_completo` | Base completa de empresas |
| `bcadastro_base_socios_consolidado` | Base consolidada de sócios |

---

## 💻 Uso

### Executar o aplicativo

```bash
streamlit run BCADASTRO.py
```

O sistema abrirá automaticamente no navegador em `http://localhost:8501`

### Primeiro Acesso

1. Insira a senha configurada (padrão: `tsevero123`)
2. Aguarde o carregamento dos dados
3. Navegue pelas páginas usando a sidebar

### Navegação

```
📊 Dashboard Executivo    → Visão geral e KPIs principais
🏆 Ranking de Grupos      → Top 100 grupos por crédito
🔬 Análise - Grupo        → Detalhamento por grupo econômico
🔍 Análise - Empresa      → Detalhamento por empresa
📄 Relatório Executivo    → Sumário consolidado
📋 Base Cadastral         → Estatísticas gerais
```

### Funcionalidades Especiais

#### Limpar Cache
Use o botão "🗑️ Limpar Cache" na sidebar para forçar atualização dos dados.

#### Filtros Interativos
- **Tema visual**: Escolha entre vários temas Plotly
- **Ação fiscal**: Filtre por tipo de irregularidade
- **Período**: Selecione períodos específicos
- **Estado**: Filtre por UF

#### Busca Avançada
- **Por Grupo**: Digite número do grupo ou CPF do sócio
- **Por Empresa**: Digite razão social ou CNPJ
- **Por Estado**: Selecione UF específica

---

## 📁 Estrutura do Projeto

```
BCadastro/
│
├── BCADASTRO.py                    # Aplicativo principal Streamlit
├── BCADASTRO.json                  # Dados de referência/backup
├── BCADASTRO-Exemplo.ipynb         # Notebook de exemplo
├── README.md                       # Documentação (este arquivo)
├── requirements.txt                # Dependências Python
│
├── .streamlit/
│   └── secrets.toml                # Credenciais (não versionado)
│
└── .git/                           # Controle de versão Git
```

---

## 🎨 Exemplos de Visualização

### Dashboard Executivo
- **KPIs**: Métricas com cards visuais e gradientes
- **Gráficos de Pizza**: Distribuição por ação fiscal
- **Gráficos de Barras**: Top 10 períodos, estados, qualificações
- **Alertas**: Cards categorizados por severidade (crítico, alto, médio, positivo)

### Análise de Grupo
- **Mapa de calor**: Distribuição geográfica
- **Gráficos de linha**: Evolução temporal
- **Tabelas interativas**: Lista de empresas e sócios

### Relatório Executivo
- **Top 50 grupos**: Priorização para fiscalização
- **Mapas**: Distribuição por estado
- **Recomendações**: Ações sugeridas por categoria

---

## 📊 Métricas e Indicadores

### Crédito Tributário
- **vl_ct**: ICMS + Juros + Multa
- **Crédito Total**: Soma de todos os grupos
- **Crédito Médio**: Média por grupo
- **Crédito Máximo**: Maior valor identificado

### Receita
- **receita_pa_fato**: Receita no período do fato gerador
- **Receita Total**: Soma de todas as empresas
- **Receita Média**: Média por empresa

### Classificações
- **EXCLUSAO_COM_DEBITO**: Irregularidade com débitos tributários (crítico)
- **EXCLUSAO_SEM_DEBITO**: Irregularidade sem débitos (alto)
- **SEM_INTERESSE**: Grupos sem irregularidades (positivo)

---

## 🔒 Segurança

### Autenticação
- Sistema protegido por senha
- Controle de sessão via `st.session_state`
- Credenciais armazenadas em arquivo não versionado

### Conexão Segura
- SSL/TLS habilitado para conexão Impala
- Autenticação LDAP
- Credenciais criptografadas em `secrets.toml`

### Boas Práticas
- Não versionar arquivo `secrets.toml`
- Alterar senha padrão em produção
- Revisar logs de acesso regularmente

---

## 🔧 Solução de Problemas

### Erro de Conexão com Impala
```
Verifique:
1. Credenciais em .streamlit/secrets.toml
2. Conectividade de rede com o servidor
3. Permissões de acesso ao database gessimples
```

### Cache não atualiza
```
Use o botão "Limpar Cache" na sidebar ou:
- Pressione 'C' no teclado
- Reinicie o servidor Streamlit
```

### Gráficos não aparecem
```
Verifique:
1. Instalação do Plotly (pip install plotly)
2. Console do navegador para erros JavaScript
3. Limpe o cache do navegador
```

### Lentidão na aplicação
```
Soluções:
1. Aumente o TTL do cache
2. Limite os registros retornados
3. Verifique a performance do banco Impala
```

---

## 📝 Notebook de Exemplo

O arquivo `BCADASTRO-Exemplo.ipynb` contém:
- Exemplos de uso interativo
- Análises exploratórias
- Testes de funções
- Visualizações customizadas

Execute no Jupyter:
```bash
jupyter notebook BCADASTRO-Exemplo.ipynb
```

---

## 🤝 Contribuição

Contribuições são bem-vindas! Para contribuir:

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/NovaFuncionalidade`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova funcionalidade'`)
4. Push para a branch (`git push origin feature/NovaFuncionalidade`)
5. Abra um Pull Request

### Padrões de Código
- Siga PEP 8 para Python
- Documente funções com docstrings
- Adicione comentários em código complexo
- Teste antes de submeter PR

---

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

---

## 👥 Autores e Créditos

### Desenvolvimento
- **Receita Estadual de Santa Catarina**
- Baseado nos scripts SQL: `bcadastro_*` (ACL Format)

### Tecnologias
- Framework: [Streamlit](https://streamlit.io/)
- Visualização: [Plotly](https://plotly.com/)
- Dados: [Apache Impala](https://impala.apache.org/)

---

## 📞 Contato e Suporte

Para questões, sugestões ou suporte:

- **Email**: receita@sef.sc.gov.br
- **Website**: https://sef.sc.gov.br
- **Issues**: Use a aba Issues do GitHub

---

## 📅 Changelog

### Versão 2.0 (Atual)
- Dashboard completo com 6 módulos
- Sistema de cache otimizado
- Autenticação integrada
- Múltiplos gráficos interativos
- Relatório executivo automatizado

### Versão 1.0
- Versão inicial com funcionalidades básicas

---

## 🎯 Roadmap

### Próximas Features
- [ ] Exportação de relatórios em PDF
- [ ] Integração com sistema de notificações
- [ ] Dashboard mobile-responsive
- [ ] API REST para integração externa
- [ ] Histórico de alterações (audit log)
- [ ] Relatórios agendados automaticamente

---

## 📚 Documentação Adicional

### Base Legal
- **Lei Complementar 123/2006** - Art. 3º, § 4º, Inciso IV
- **Resolução CGSN** - Regras do Simples Nacional
- **Instruções Normativas SEF/SC** - Procedimentos fiscais

### Glossário
- **TE**: Termo de Exclusão
- **CNPJ**: Cadastro Nacional de Pessoa Jurídica
- **CPF**: Cadastro de Pessoa Física
- **CNAE**: Classificação Nacional de Atividades Econômicas
- **UF**: Unidade Federativa
- **PA**: Período de Apuração
- **CT**: Crédito Tributário

---

## 🌟 Agradecimentos

Agradecimentos especiais à equipe da Receita Estadual de Santa Catarina pelo desenvolvimento e manutenção deste sistema, contribuindo para a transparência e eficiência da fiscalização tributária.

---

<div align="center">

**GENESIS v2.0** - Receita Estadual de Santa Catarina
Desenvolvido com ❤️ em Python + Streamlit

[⬆ Voltar ao topo](#-genesis---sistema-de-análise-de-grupos-econômicos)

</div>
