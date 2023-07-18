# %%
import utils
# %%
keysdb = []
# %%
df_read = utils.query_mysql('autor', keysdb )
df_read
# %%
df_faker = utils.faker_generate(2,'PANDAS')
df_faker



# # %%

# df_read.loc[0:0].to_dict(orient='records')

# # %%
# df_x = df_read.loc[0:0]
# df_x.loc[0,'cargo'] = 'Engenheiro'
# %%

utils.send_mysql('clientes', df_faker, keysdb,'UPSERT')
# %%
keysdb = {
        'host':'localhost',
        'port':'3307',
        'database':'db_escola',
        'user':'pycode',
        'password':'admin123'}

utils.query_mysql('alunos', keysdb)
# %%
