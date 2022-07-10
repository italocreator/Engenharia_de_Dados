# --------------------------------
#           PySpark
# --------------------------------


# Imprime os datatypes das colunas do DF

df.printSchema()

# Número de linhas

df.count()

# Exibe 5 primeiras linhas

df.show(5)

# Valores únicos de uma coluna

df.select('Coluna').distinct()

# Ordenando o DF

df.orderBy(col('Coluna').desc())

# Renomear coluna

df.withColumnRenamed('Nome_Coluna', 'Novo_Nome_Coluna')

# Mudando o tipo de uma coluna

df.withColumn('Coluna', col('Coluna').cast('Float'))

# Substituindo valores de uma coluna

df.withColumn('Coluna', regex_replace(col('Coluna'), 'Valor', 'Novo_Valor'))

# Criando novas colunas
df = df.withColumn(
    "faixa_etaria",
    when(col("Idade") < 19, "Jovem")
    .when((col("Idade") >= 20) & (col("Idade") <= 59), "Adulto")
    .when(col("Idade") >= 60, "Idoso"),
)

# Removendo uma ou mais colunas

df.drop("Experience_Years")
df.drop("Experience_Years", "ID")

# Filtrando valores

# Filter
df.filter(df.Salary > 40000)
df.filter((df.Salary > 40000) & (df.Gender == "Female"))

# Where
df.where(df.Salary > 40000)
df.where((df.Salary > 40000) & (df.Gender == "Female"))

# Select e Where

df.select(df.Age, df.Gender, df.Salary).where(df.Salary > 400000)

# GroupBy

# Somatória do salário por sexo
df.groupBy("Gender").sum("Salary").withColumnRenamed("sum(Salary)", "Sum")

# Média de salário por faixa etária
df.groupBy("Age_Group").mean(
    "Salary").withColumnRenamed("mean(Salary)", "Mean")
