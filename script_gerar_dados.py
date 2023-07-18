# %%
import mysql.connector
from mysql.connector import Error
import pandas as pd
from faker import Faker
faker = Faker('pt_BR')

#%%

def conexao(keysdb:str):
    """Realiza a conexao com o banco inserido chamado pelas outras funcoes

    Args:
        host (str): Hostnames do SGBD
        database (str): Banco de Dados
        user (str): usuario
        password (str): senha
    """    
    connection = mysql.connector.connect(host = keysdb[0],
                                         database= keysdb[1],
                                         user = keysdb[2],
                                         password= keysdb[3]
                                         )
    return(connection)


def insert_mysql(table, df, keysdb ):
    """_summary_

    Args:
        table (_type_): _description_
        df (_type_): _description_
        keysdb (_type_): _description_
    """    
    try:
        connection = conexao(keysdb)
        cursor = connection.cursor()

        cols = ', '.join(df.columns)
        records = [tuple(row) for row in df.values]

        query = f"INSERT INTO {table} ({cols}) VALUES ({', '.join(['%s'] * len(df.columns))})"
        
        
        cursor.executemany(query, records)
        connection.commit()
 
        print(f'''Records inserted successfully 
                into {df.data_atualizacao.count()} registers in {table} table'''
                )

    except mysql.connector.Error as error:
        print("Failed to insert into MySQL table {}".format(error))

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# %%
# Dados para Conexão 
keysdb = ['localhost','db_unidade3','root','admin']


# %%
# Loop para criar as linhas de dados e adicionas a uma lista
dados = []
for i in range(100):
    data_atualizacao = faker.date()
    nome = faker.name()
    telefone = faker.phone_number()
    email = faker.ascii_free_email()
    data_nascimento = faker.date()
    rua = faker.street_address()
    bairro = faker.bairro()
    cidade = faker.administrative_unit()
    estado = faker.state()
    estado_sigla = faker.estado_sigla()
    cargo = faker.job()
    salary = faker.job()
    # coments = faker.text()
    # ipv4 = faker.ipv4_private()
    
    
    
    dados_faker = (
                   data_atualizacao,
                   nome,
                   telefone,
                   email, 
                   data_nascimento,
                   rua,
                   bairro,
                   cidade, 
                   estado, 
                   estado_sigla,
                   cargo,
                   salary )

    dados.append(dados_faker)

# Colunas do Dataframe
cols = [
        'data_atualizacao',
        'nome',
        'telefone',
        'email', 
        'data_nascimento',
        'rua',
        'bairro',
        'cidade',
        'estado', 
        'estado_sigla',
        'cargo',
        'salary'
        ]

# Criando o DF e nomeando as colunas
df = pd.DataFrame(dados, columns=cols)
df
# %%
# insert_mysql('clientes',df, keysdb)
# %%
