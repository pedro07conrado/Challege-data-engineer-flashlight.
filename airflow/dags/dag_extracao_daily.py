from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import os
import shutil
from sqlalchemy import create_engine

# Configuração padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Definição do DAG
with DAG(
    'data_pipeline',
    default_args=default_args,
    description='Pipeline de dados simples',
    schedule_interval=timedelta(days=1),
) as dag:

    # Função para extrair dados do PostgreSQL
    def extract_postgres():
        conn = psycopg2.connect("dbname='northwind' user='postgres' host='localhost' password='nova_senha'")
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        tables = cursor.fetchall()
        print(tables)

        for table in tables:
            table_name = table[0]
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
            print(df)
            os.makedirs(f'data/postgres/{table_name}', exist_ok=True)
            df.to_csv(f'data/postgres/{table_name}/data.csv', index=False)
            print(f"Tabela {table_name} salva com sucesso.")
        conn.close()

    def extract_csv():
        # Definir caminho do arquivo de entrada e diretório de saída
        input_file = "/home/pedro/airflow/data/csv/order_details.csv"
        output_dir = "/home/pedro/airflow/data/csv/processed_order_details"
        
        # Debug: Imprimir caminho do arquivo e conteúdo do diretório
        print(f"Verificando a existência do arquivo: {input_file}")
        print("Conteúdo do diretório /home/pedro/airflow/data/csv:")
        print(os.listdir("/home/pedro/airflow/data/csv"))

        # Verificar se o arquivo de entrada existe
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Arquivo não encontrado: {input_file}")

        # Criar diretório de destino (se não existir)
        os.makedirs(output_dir, exist_ok=True)

        # Copiar o arquivo CSV para o diretório de destino
        shutil.copy(input_file, f"{output_dir}/order_details.csv")
        print(f"Arquivo copiado para: {output_dir}/order_details.csv")

    def save_db(df: pd.DataFrame, nome_df:str):
            # Dados de conexão com o banco de dados
        usuario = 'postgres'
        senha = 'nova_senha'
        host = 'localhost'
        port = '5432'
        database = 'final_database'

        # Cria a string de conexão
        string_conexao = f'postgresql+psycopg2://{usuario}:{senha}@{host}:{port}/{database}'

        # Cria o motor de conexão
        engine = create_engine(string_conexao)

            # Testa a conexão
        try:
            with engine.connect() as connection:
                print("Conexão estabelecida com sucesso!")
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")
        df.to_sql(nome_df, engine, if_exists='replace', index=False)


    def load_to_postgres():
    # Definindo caminhos dos arquivos
        orders_path = 'data/postgres/orders/data.csv'
        products_path = 'data/postgres/products/data.csv'
        customers_path = 'data/postgres/customers/data.csv'
        order_details_path = 'data/csv/order_details.csv'
        categories_path = 'data/postgres/categories/data.csv'
        employees_path = 'data/postgres/employees/data.csv'
        shippers_path = 'data/postgres/shippers/data.csv'
        suppliers_path = 'data/postgres/suppliers/data.csv'
        territories_path = 'data/postgres/territories/data.csv'
        employee_territories_path = 'data/postgres/employee_territories/data.csv'
        
        #Ler as outras pastas aqui
        #TODO:
    
        print(os.getcwd())
 
        # Ler os dados extraídos
        orders = pd.read_csv(orders_path)
        products = pd.read_csv(products_path)
        customers = pd.read_csv(customers_path)
        order_details = pd.read_csv(order_details_path)
        categories = pd.read_csv(categories_path)
        employees = pd.read_csv(employees_path)
        shippers = pd.read_csv(shippers_path)
        suppliers = pd.read_csv(suppliers_path)
        territories = pd.read_csv(territories_path)
        employee_territories = pd.read_csv(employee_territories_path)

        #TODO: Ler os CSVs
        
        # Verificar colunas
        print("Colunas em orders:", orders.columns)
        print("Colunas em products:", products.columns)
        print("Colunas em customers:", customers.columns)
        print("Colunas em order_details:", order_details.columns)

        lista_dataframes = []
        lista_dataframes.append((orders,"orders"))
        lista_dataframes.append((products,"products"))
        lista_dataframes.append((customers,"customers"))
        lista_dataframes.append((order_details, "order_details"))
        

        for df,nome in lista_dataframes:
            save_db(df,nome)

        


    # Definição das tarefas no DAG
    t1 = PythonOperator(
        task_id='extract_postgres',
        python_callable=extract_postgres,
    )

    t2 = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_csv,
    )

    t3 = PythonOperator(
        task_id='transform_save',
        python_callable=load_to_postgres,
    )

    # Definindo a ordem de execução das tarefas
    t1 >> t2
    t2 >> t3
    