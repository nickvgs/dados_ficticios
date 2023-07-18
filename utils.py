# %%
import mysql.connector
from mysql.connector import connection
from mysql.connector import Error
import pandas as pd
from faker import Faker
import random
import decimal
import data_list as dl

faker = Faker('pt_BR')

#%%
# Função de Conexão 
def conexao(keys:dict):
    """Realiza a conexao com o banco inserido chamado pelas outras funcoes

    Args:
        host (str): Hostnames do SGBD
        database (str): Banco de Dados
        user (str): usuario
        password (str): senha
    """    
    con = connection.MySQLConnection(**keys)

    return(con)
# %%
# Função de Consulta 
def query_mysql(table, keys:dict):
    """ Realiza a consulta no banco de dados MySQL

    Args:
        table (_type_): _description_
        keysdb (list): _description_
    """    

    try:
            
        con = conexao(keys)
        cursor = con.cursor()

        query = f"SELECT * FROM {table}"

        cursor.execute(query)
        data = cursor.fetchall()
        
        column_names = [description[0] for description in cursor.description]
        
        dados = pd.DataFrame(data, columns=column_names)

    except mysql.connector.Error as error:
        print("Failed to query into MySQL table {}".format(error))

    finally:
            if con.is_connected():
                cursor.close()
                con.close()
                print("MySQL connection is closed")
    return(dados)

# %%
#Função de INSERT e UPDATE (UPSERT)
def send_mysql(table, df, keysdb, action="INSERT"):

    # Create a connection to the database
    con = conexao(keysdb)

    # Convert the dataframe to a list of dictionaries
    data = df.to_dict(orient='records')
    params = [tuple(row.values()) for row in data]

    placeholders = ', '.join(['%s'] * len(df.columns))

    # Perform the update
    with con.cursor() as cursor:
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
    con.commit()

    # Close the connection
    con.close()


# %%
# Função de gerar dados ficticios de clientes

def gen_clients(quantidade:int, output='pandas'):
    
    # Loop para criar as linhas de dados e adicionas a uma lista
    dados = []
    for i in range(quantidade):
        data_atualizacao = (faker.date_of_birth(maximum_age=1)).strftime("%Y-%m-%d")
        nome = faker.name()
        telefone = faker.phone_number()
        email = faker.ascii_free_email()
        data_nascimento = (faker.date_of_birth(maximum_age=80)).strftime("%Y-%m-%d")
        rua = faker.street_address()
        bairro = faker.bairro()
        cidade = faker.administrative_unit()
        cargo = faker.job()
        salario = round(random.uniform(1300.15, 6300.89), 2)
        
        estadoSigla = faker.estado()
        estado_sigla = estadoSigla[0]
        estado = estadoSigla[1]
        
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

    if output == 'lista':
        return(dados)
    elif output == 'pandas':
        return(df)



# %%
# Função de gerar dados ficticios de carros
def gen_cars(quantidade:int, output):
    

    dados = []
    for i in range(quantidade):
        data_atualizacao = faker.date()
        ano_modelo = (faker.date_of_birth(maximum_age=10)).strftime("%Y")
        cor = faker.safe_color_name().title()
        placa = faker.license_plate()
        marca = dl.marcas_carros[random.randrange(0,len(dl.marcas_carros))]
        valor = round(random.uniform(15000.50, 65000.50), 2)
        
        estadoSigla = faker.estado()
        estado_sigla = estadoSigla[0]
        estado = estadoSigla[1]

        dados_faker = (
                    data_atualizacao,
                    ano_modelo,
                    cor,
                    placa,
                    estado,
                    estado_sigla,
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
            'estado_emp',
            'UF_emp',
            'marca',
            'valor'
            ]

    # # Criando o DF e nomeando as colunas
    df = pd.DataFrame(dados, columns=cols)

    if output == 'lista':
        return(dados)
    elif output == 'pandas':
        return(df)
# %%
# Função de gerar dados ficticios de trafego de rede
def gen_trafic_networks(quantidade:int, output='pandas'):
    

    dados = []
    for i in range(quantidade):
        data_acesso = faker.date()
        uri_acesso = faker.uri()
        ipv4_origem = faker.ipv4_public()
        ipv6_origem = faker.ipv6()
        # estado_origem = faker.estado_nome()
        hostname = faker.hostname()
        email = faker.free_email()
        quantidade_acesso = random.randint(2,358)

        estadoSigla = faker.estado()
        estado_sigla = estadoSigla[0]
        estado = estadoSigla[1]
        
        dados_faker = (
                    data_acesso,
                    uri_acesso,
                    ipv4_origem,
                    ipv6_origem,
                    hostname,
                    estado,
                    estado_sigla,
                    email,
                    quantidade_acesso

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
            'UF_origem'
            'email',
            'quantidade_acesso'
            ]

    # # Criando o DF e nomeando as colunas
    df = pd.DataFrame(dados, columns=cols)

    if output == 'lista':
        return(dados)
    elif output == 'pandas':
        return(df)
# %%
# Função de gerar dados ficticios livros
def gen_books(quantidade:int, output='pandas'):
    

    dados = []
    for i in range(quantidade):
        ano_publicacao = faker.year()
        nome_livro = faker.catch_phrase()
        autor = faker.name()
        estoque = random.randint(1,18)
        valor = round(random.uniform(12.32, 126.50), 2)


        
        dados_faker = (
                    ano_publicacao,
                    nome_livro,
                    autor,
                    estoque,
                    valor
                   )

        dados.append(dados_faker)

    # Colunas do Dataframe
    cols = [
            'ano_pub',
            'nome_livro',
            'autor',
            'estoque',
            'valor'
            ]



    if output == 'lista':
        return(dados)
    elif output == 'pandas':
        # Criando o DF e nomeando as colunas
        df = pd.DataFrame(dados, columns=cols)
        return(df)
# %%
def gen_products(quantidade:int, output='pandas', index=None):
    
    ix = 0
    if index == None:
        ix == 0
    else:
        ix = index
   

    dados = []
    for i in range(quantidade):
        id = ix + i
        data_fabricacao = (faker.date_of_birth(maximum_age=5)).strftime("%Y-%m-%d")
        produto = dl.produtos[random.randrange(0,len(dl.produtos))]
        estoque = random.randint(1,18)
        valor = round(random.uniform(12.32, 126.50), 2)
        garantia = random.randint(1,18)

        if index == None:
            dados_faker = (                     
                    data_fabricacao,
                    produto,
                    estoque,
                    valor,
                    garantia
                   )
        else:
            dados_faker = (  
                    id,                   
                    data_fabricacao,
                    produto,
                    estoque,
                    valor,
                    garantia
                   )

        dados.append(dados_faker)

        

    if output == 'lista':
        return(dados)
    elif output == 'pandas':

        if index == None:   
    
            cols = [
                    'data_fabricacao',
                    'produto',
                    'estoque',
                    'valor',
                    'garantia_meses'
                    ]

        else:
            cols = [
                    'id',
                    'data_fabricacao',
                    'produto',
                    'estoque',
                    'valor',
                    'garantia_meses'
                    ]
        df = pd.DataFrame(dados, columns=cols)
        return(df)
# %%
