# %%
import mysql.connector
from mysql.connector import Error
import pandas as pd
from faker import Faker
import random
import decimal
import data_list as dl

faker = Faker('pt_BR')

#%%
# Funcções de Negócio
def conexao(keysdb:str):
    """Realiza a conexao com o banco inserido chamado pelas outras funcoes

    Args:
        host (str): Hostnames do SGBD
        database (str): Banco de Dados
        user (str): usuario
        password (str): senha
    """    
    connection = mysql.connector.connect(host = keysdb[0],
                                         port = keysdb[1],
                                         database= keysdb[2],
                                         user = keysdb[3],
                                         password= keysdb[4]
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


def query_mysql(table, keysdb:list):
    """ Realiza a consulta no banco de dados MySQL

    Args:
        table (_type_): _description_
        keysdb (list): _description_
    """    

    try:
            
        connection = conexao(keysdb)
        cursor = connection.cursor()

        query = f"SELECT * FROM {table}"

        cursor.execute(query)
        data = cursor.fetchall()
        
        column_names = [description[0] for description in cursor.description]
        
        dados = pd.DataFrame(data, columns=column_names)

    except mysql.connector.Error as error:
        print("Failed to query into MySQL table {}".format(error))

    finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed")
    return(dados)

# %%

def send_mysql(table, df, keysdb, action="INSERT"):

    # Create a connection to the database
    connection = conexao(keysdb)

    # Convert the dataframe to a list of dictionaries
    data = df.to_dict(orient='records')
    params = [tuple(row.values()) for row in data]

    placeholders = ', '.join(['%s'] * len(df.columns))

    # Perform the update
    with connection.cursor() as cursor:
        if action == "UPSERT":
            columns = [column for column in df.columns if column != "id"]
            sql = f'''
                INSERT INTO {table} ({', '.join(df.columns)}) 
                VALUES ({placeholders}) 
                ON DUPLICATE KEY UPDATE {', '.join([f'{column}=VALUES({column})' for column in columns])}
            '''
            # print(sql)
            # print(params)
            cursor.executemany(sql, params)

        elif action == "INSERT":
            
            sql = f"INSERT INTO {table} ({', '.join(df.columns)}) VALUES ({placeholders})"
            cursor.executemany(sql, params)

            # print(sql)
            # print(params)
        else:
            raise Exception("Invalid action")

    # Commit the changes
    connection.commit()

    # Close the connection
    connection.close()


# %%
def generate_clients(qt:int, output):
    
    # Loop para criar as linhas de dados e adicionas a uma lista
    dados = []
    for i in range(qt):
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
        salario = round(random.uniform(1300.15, 6300.89), 2)
        
        
        
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
                    salario

                    )

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
            'salario'
            ]

    # # Criando o DF e nomeando as colunas
    df = pd.DataFrame(dados, columns=cols)

    if output == 'LISTA':
        return(dados)
    elif output == 'PANDAS':
        return(df)



# %%

def generate_cars(qt:int, output):
    

    dados = []
    for i in range(qt):
        data_atualizacao = faker.date()
        ano_modelo = faker.year()
        cor = faker.safe_color_name().title()
        placa = faker.license_plate()
        marca = dl.marcas_carros[random.randrange(0,len(dl.marcas_carros))]
        valor = round(random.uniform(15000.50, 65000.50), 2)
        
        dados_faker = (
                    data_atualizacao,
                    ano_modelo,
                    cor,
                    placa,
                    marca,
                    valor
                   )

        dados.append(dados_faker)

    # Colunas do Dataframe
    cols = [
            'data_atualizacao',
            'ano_modelo',
            'cor',
            'placa',
            'marca',
            'valor'
            ]

    # # Criando o DF e nomeando as colunas
    df = pd.DataFrame(dados, columns=cols)

    if output == 'LISTA':
        return(dados)
    elif output == 'PANDAS':
        return(df)
# %%
def generate_trafic_networks(qt:int, output):
    

    dados = []
    for i in range(qt):
        data_acesso = faker.date()
        uri_acesso = faker.uri()
        ipv4_origem = faker.ipv4_public()
        ipv6_origem = faker.ipv6()
        estado_origem = faker.estado_nome()
        hostname = faker.hostname()
        email = faker.free_email()
        qt_acesso = random.randint(2,358)

        
        dados_faker = (
                    data_acesso,
                    uri_acesso,
                    ipv4_origem,
                    ipv6_origem,
                    hostname,
                    estado_origem,
                    email,
                    qt_acesso

                   )

        dados.append(dados_faker)

    # Colunas do Dataframe
    cols = [
            'data_acesso',
            'uri_acesso',
            'ipv4_origem',
            'ipv6_origem',
            'hostname',
            'estado_origem',
            'email',
            'qt_acesso'
            ]

    # # Criando o DF e nomeando as colunas
    df = pd.DataFrame(dados, columns=cols)

    if output == 'LISTA':
        return(dados)
    elif output == 'PANDAS':
        return(df)
# %%
