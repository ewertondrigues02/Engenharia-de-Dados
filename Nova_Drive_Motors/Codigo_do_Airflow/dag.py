from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='postgres_to_snowflake',
    default_args=default_args,
    description='Carregar dados incrementalmente do Postgres para o Snowflake',
    schedule_interval=timedelta(days=1),
    catchup=False
)
def postgres_to_snowflake_etl():
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'vendas', 'clientes']

    tasks = []
    for table_name in table_names:
        @task(task_id=f'get_max_id_{table_name}')
        def get_max_primary_key(table_name: str):
            with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f'SELECT MAX(ID_{table_name}) FROM {table_name}')
                    max_id = cursor.fetchone()[0]
                    return max_id if max_id is not None else 0

        @task(task_id=f'load_data_{table_name}')
        def load_incremental_data(table_name: str, max_id: int):
            with PostgresHook(postgres_conn_id='postgres').get_conn() as pg_conn:
                with pg_conn.cursor() as pg_cursor:
                    primary_key = f'ID_{table_name}'

                    pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                    columns = [row[0] for row in pg_cursor.fetchall()]
                    columns_list_str = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(columns))

                    pg_cursor.execute(f"SELECT {columns_list_str} FROM {table_name} WHERE {primary_key} > {max_id} ")
                    rows = pg_cursor.fetchall()

                    with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as sf_conn:
                        with sf_conn.cursor() as sf_cursor:
                            insert_query = f"INSERT INTO {table_name} ({columns_list_str}) VALUES ({placeholders})"
                            for row in rows:
                                sf_cursor.execute(insert_query, row)

        max_id_task = get_max_primary_key(table_name)
        load_data_task = load_incremental_data(table_name, max_id_task)
        tasks.append(load_data_task)

    create_snowflake_tables = SnowflakeOperator(
        task_id='create_snowflake_tables',
        sql=[
            f"CREATE TABLE IF NOT EXISTS veiculos ( id_veiculos INTEGER, nome VARCHAR(255) NOT NULL, tipo VARCHAR(100) NOT NULL, valor DECIMAL(10, 2) NOT NULL, data_atualizacao TIMESTAMP_LTZ, data_inclusao TIMESTAMP_LTZ)",  
            f"CREATE TABLE IF NOT EXISTS estados (id_estados INTEGER, estado VARCHAR(100) NOT NULL, sigla CHAR(2) NOT NULL, data_inclusao TIMESTAMP_LTZ, data_atualizacao TIMESTAMP_LTZ)",
            f"CREATE TABLE IF NOT EXISTS cidades ( id_cidades INTEGER, cidade VARCHAR(255) NOT NULL, id_estados INTEGER NOT NULL, data_inclusao TIMESTAMP_LTZ, data_atualizacao TIMESTAMP_LTZ)", 
            f"CREATE TABLE IF NOT EXISTS concessionarias ( id_concessionarias INTEGER, concessionaria VARCHAR(255) NOT NULL, id_cidades INTEGER NOT NULL, data_inclusao TIMESTAMP_LTZ, data_atualizacao TIMESTAMP_LTZ)",  
            f"CREATE TABLE IF NOT EXISTS vendedores (id_vendedores INTEGER, nome VARCHAR(255) NOT NULL, id_concessionarias INTEGER NOT NULL, data_inclusao TIMESTAMP_LTZ, data_atualizacao TIMESTAMP_LTZ)",
            f"CREATE TABLE IF NOT EXISTS vendas ( id_vendas INTEGER, id_veiculos INTEGER NOT NULL, id_concessionarias INTEGER NOT NULL, id_vendedores INTEGER NOT NULL, id_clientes INTEGER NOT NULL, valor_pago DECIMAL(10, 2) NOT NULL, data_venda TIMESTAMP_LTZ, data_inclusao TIMESTAMP_LTZ, data_atualizacao TIMESTAMP_LTZ)",
            f"CREATE TABLE IF NOT EXISTS clientes ( id_clientes INTEGER, cliente VARCHAR(255) NOT NULL, endereco TEXT NOT NULL, id_concessionarias INTEGER NOT NULL, data_inclusao TIMESTAMP_LTZ, data_atualizacao TIMESTAMP_LTZ)"
        ],
        snowflake_conn_id='snowflake'
    )

    tasks.append(create_snowflake_tables)

    return tasks

postgres_to_snowflake_etl_dag = postgres_to_snowflake_etl()
