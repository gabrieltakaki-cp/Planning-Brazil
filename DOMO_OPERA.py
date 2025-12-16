import warnings
import os
import numpy as np
from datetime import datetime
import pandas as pd
from pydomo import Domo
import time
import sqlite3

# import domojupyter as domojupyter

# Ignorar todos os warnings
warnings.filterwarnings('ignore')

cliente_id = os.environ['client_id']
secret = os.environ['client_secret']

domo = Domo(cliente_id, secret, api_host='api.domo.com')
input_dataset = '2fdb6825-1f2d-4742-8f00-73531ef9183a'
output_dataset = '2982850a-98d5-4b83-ad2b-591b56bd61df'

sqlite_file = 'database_dfr.db'
table_name = 'TEMP'

# Remove o banco de dados antigo se ele existir, para garantir uma carga limpa
if os.path.exists(sqlite_file):
    os.remove(sqlite_file)

# Cria a conexão com o banco de dados SQLite
conn_sqlite = sqlite3.connect(sqlite_file)

sql = f"""SELECT LINE_NAME,
    PLANT,
    GH_AREA,
    GH_CATEGORY,
    PHYSICAL_AREA,
    SHORT_MATERIAL_ID,
    UOM_ST_SAP,
    MATERIAL_UOM,
    PRODUCTION_ORDER_RATE,
    RAMPUP_FLAG,
    PRODUCTIONDATE_DAY_LOC,
    GOOD_PRODUCTION_QTY,
    SIZE_TYPE FROM TABLE WHERE PLANT IN ('BR10','BR12') AND GOOD_PRODUCTION_QTY > 0"""


transactions = domo.ds_query(input_dataset, query=sql)
transactions.to_sql(table_name, conn_sqlite, if_exists='append', index=False)
print(f" -> Sucesso. {len(transactions)} linhas salvas localmente.")

print("Obtendo colunas!")
colunas = [col.upper() for col in transactions.columns.to_list()]
print("Colunas obtidas!")

print("Criando dataframe vazio!")
empty_df = pd.DataFrame(columns=colunas)  # Gera um dataframe vazio.
print("Dataframe vazio criado!")

print("Transformando dataframe vazio em csv!")
empty_csv = empty_df.to_csv(index=False)  # Gera um csv
print("Dataframe vazio transformado em csv!")

# Limpa o dataset API final antes da carga.
print("Limpando dataset do DOMO!")
domo.datasets.data_import(output_dataset, empty_csv, update_method='REPLACE')
print("Dataset do DOMO limpo!")

chunk_size = 500000  # Define o tamanho do chunk (ajuste conforme necessário)
sql_tabela_final = f'SELECT * FROM {table_name}'

try:
    # Itera sobre os dados do SQLite em chunks
    for chunk in pd.read_sql(sql_tabela_final, con=conn_sqlite, chunksize=chunk_size):
        # Garante que as colunas do chunk estejam em maiúsculas
        chunk.columns = [col.upper() for col in chunk.columns]

        # Envia o chunk para o Domo
        domo.datasets.data_import(output_dataset, chunk.to_csv(index=False), update_method='APPEND')
        print(f" -> Gravado chunk com {len(chunk)} linhas no Domo")
finally:
    # Garante que as conexões sejam fechadas no final
    conn_sqlite.close()
    print("\nConexão SQLite encerrada.")
    print("Upload no DOMO finalizado com sucesso! ✅")
