# Etapa 1: Análise Exploratória
Fases da Etapa:
    •	Conectando ao banco de dados Nova Drive Motors no Postgres.
    •	Conhecer as tabelas.
    •	Explorar os dados das tabelas.
    •	Ver relações entre as tabelas.
    •	Para administrar o banco foi usado o Postgres.


# Etapa 2:  Configurar o Airflow 
Fase da Etapa:
    •	Foi criado uma máquina virtual EC2 na AWS.
    •	Instalado o Ubuntu Server 22.04 LTS SSD. Tipo de máquina t2.large.
    •	No Ubunto Server foram instalados o Docker e o Airflow.

# Etapa 3: Configurar Snowflake
Fase da Etapa: 
    •	Conectar conta do Snowflake.
    •	Foi criado os objetos como: banco de dados, schemas, warehouse.
    •	Foi criado também tabelas trazidas de forma de dinâmica do Airflow.

# Etapa 4: Criando Stage com Airflow
Fase da Etapa:
    •	Foi criado o pipeline de dados.
    •	Testado a carga dos dados no airflow.
    •	Explorados os dados no snowflake.
    •	Foi criado DAG com as Task.

# Etapa 5: Criar e Configurar Camada Análitica com dbt
Fase da Etapa:
    •	Criado Diversos modelos como: Models, Dimension, stage, facts e test.
    •	Rodar os Jobs.
    •	Conectar o dbt com Snowflake.
	
# Etapa 6: Dashboard no Looker Studio
Fase da Etapa:
    •	Conectar o data warehouse do snowflake com Lookerstudio.
    •	Criar Dashboard da camada Analitica do datawarehouse.

## Grafo

![linhagem dos grafos](https://github.com/ewertondrigues02/Engenharia-de-Dados/assets/106437473/6f4434fd-74d4-4077-83aa-978e99a74121)





