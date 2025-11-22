# 📝 Notas Técnicas - Gerador de Data Schemas

## ⚠️ Problema Resolvido: Conflito com `sum()` do PySpark

### 🐛 Descrição do Problema

Quando você usa:
```python
from pyspark.sql.functions import *
```

A função `sum()` **nativa do Python** é **substituída** pela função `pyspark.sql.functions.sum()`, que espera uma **coluna PySpark** como argumento, não números ou listas.

### ❌ Exemplo do Erro

```python
from pyspark.sql.functions import *

# Isso FALHA com PySparkTypeError
lista1 = [1, 2, 3]
lista2 = [4, 5, 6]
total = sum([len(lista1), len(lista2)])  # ❌ ERRO!
# PySparkTypeError: [NOT_COLUMN_OR_STR] Argument `col` should be a Column or str, got int.
```

**Por quê?** Porque `sum()` agora é `pyspark.sql.functions.sum()` que espera uma coluna, não números.

### ✅ Solução Implementada

#### Opção 1: Usar `builtins.sum()` (Recomendado)

```python
from pyspark.sql.functions import *
import builtins  # Importar módulo builtins

# Funciona! Usa a função sum() nativa do Python
lista1 = [1, 2, 3]
lista2 = [4, 5, 6]
total = builtins.sum([len(lista1), len(lista2)])  # ✅ OK!
```

#### Opção 2: Calcular explicitamente

```python
from pyspark.sql.functions import *

# Funciona! Soma explícita
lista1 = [1, 2, 3]
lista2 = [4, 5, 6]
total = len(lista1) + len(lista2)  # ✅ OK!
```

#### Opção 3: Importar seletivamente (Alternativa)

```python
# Importar apenas as funções que você precisa
from pyspark.sql.functions import col, count, avg  # etc...

# sum() continua sendo a função nativa
lista1 = [1, 2, 3]
lista2 = [4, 5, 6]
total = sum([len(lista1), len(lista2)])  # ✅ OK!
```

---

## 📚 Implementação nos Notebooks

### `gerar_data_schemas.ipynb`

```python
# ✅ Célula de Imports
import os
import json
import builtins  # IMPORTANTE: Para usar sum() nativo do Python
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *  # Isso sobrescreve sum()

# ✅ Uso correto ao longo do notebook
total_tabelas = builtins.sum([len(TABELAS_ORIGINAIS), len(TABELAS_INTERMEDIARIAS)])
```

### `gerar_schemas_simples.ipynb`

```python
# ✅ Célula de Configuração
import os
import builtins  # IMPORTANTE: Para evitar conflito com pyspark.sql.functions.sum()
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ✅ Uso correto
total = builtins.sum([len(TABELAS['originais']), len(TABELAS['intermediarias'])])
```

---

## 🔍 Onde Usar `builtins.sum()`

### ✅ Use `builtins.sum()` quando:

1. **Somar números inteiros:**
   ```python
   total = builtins.sum([1, 2, 3, 4, 5])
   ```

2. **Somar comprimentos de listas:**
   ```python
   total_itens = builtins.sum([len(lista1), len(lista2), len(lista3)])
   ```

3. **Qualquer operação com números nativos do Python:**
   ```python
   total_arquivos = builtins.sum([sucesso, falha])
   ```

### ✅ Use `sum()` do PySpark quando:

1. **Somar valores de uma coluna DataFrame:**
   ```python
   from pyspark.sql.functions import sum

   df.select(sum("valor_coluna")).show()
   ```

2. **Agregações em DataFrames:**
   ```python
   df.groupBy("categoria").agg(sum("quantidade"))
   ```

---

## 📊 Tabela Comparativa

| Operação | Função Correta | Exemplo |
|----------|---------------|---------|
| Somar lista de números | `builtins.sum()` | `builtins.sum([1, 2, 3])` |
| Somar comprimentos | `builtins.sum()` | `builtins.sum([len(a), len(b)])` |
| Somar coluna DataFrame | `pyspark.sql.functions.sum()` | `df.select(sum("coluna"))` |
| Agregação DataFrame | `pyspark.sql.functions.sum()` | `df.agg(sum("valor"))` |

---

## 🎯 Boas Práticas

### ✅ Recomendado

```python
import builtins
from pyspark.sql.functions import *

# Para números/listas
resultado = builtins.sum([10, 20, 30])

# Para DataFrames
df.select(sum("coluna")).show()
```

### ⚠️ Alternativa (mais verbosa)

```python
from pyspark.sql.functions import col, count, avg, sum as spark_sum

# Para números/listas
resultado = sum([10, 20, 30])  # sum() nativo

# Para DataFrames
df.select(spark_sum("coluna")).show()  # spark_sum()
```

### ❌ Evitar

```python
from pyspark.sql.functions import *

# ERRO! sum() agora é do PySpark
resultado = sum([10, 20, 30])  # ❌ PySparkTypeError
```

---

## 🔧 Outros Conflitos Comuns

Além de `sum()`, outras funções do Python podem ser sobrescritas:

| Função Python | Função PySpark | Solução |
|--------------|----------------|---------|
| `sum()` | `pyspark.sql.functions.sum()` | `builtins.sum()` |
| `min()` | `pyspark.sql.functions.min()` | `builtins.min()` |
| `max()` | `pyspark.sql.functions.max()` | `builtins.max()` |
| `abs()` | `pyspark.sql.functions.abs()` | `builtins.abs()` |
| `round()` | `pyspark.sql.functions.round()` | `builtins.round()` |

### Exemplo Completo

```python
import builtins
from pyspark.sql.functions import *

# ✅ Funções nativas do Python
lista = [10, -5, 20, -15]
total = builtins.sum(lista)           # Soma
minimo = builtins.min(lista)          # Mínimo
maximo = builtins.max(lista)          # Máximo
absoluto = builtins.abs(-10)          # Valor absoluto
arredondado = builtins.round(3.7)     # Arredondamento

# ✅ Funções PySpark em DataFrames
df.select(
    sum("valor"),
    min("valor"),
    max("valor"),
    abs(col("valor")),
    round(col("valor"), 2)
).show()
```

---

## 📚 Referências

### Documentação Oficial

- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Python Built-in Functions](https://docs.python.org/3/library/functions.html)
- [Python builtins Module](https://docs.python.org/3/library/builtins.html)

### Mensagens de Erro Comuns

```
PySparkTypeError: [NOT_COLUMN_OR_STR] Argument `col` should be a Column or str, got int.
```

**Causa:** Tentou usar `sum()` do PySpark com números/listas ao invés de colunas.

**Solução:** Use `builtins.sum()` para números/listas.

---

## 💡 Dica Final

**Sempre importe `builtins` quando usar `from pyspark.sql.functions import *`:**

```python
import builtins  # ← SEMPRE adicione isso!
from pyspark.sql.functions import *
```

Isso garante que você sempre terá acesso às funções nativas do Python sem conflitos.

---

**Criado em:** 2025-11-17
**Versão:** 1.0
**Aplicação:** Gerador de Data Schemas BCadastro
