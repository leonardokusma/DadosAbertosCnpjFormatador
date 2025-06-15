import pandas as pd
import Config as c
import ConnectionFactory as bd
import Repository.EmpresasTable as Empresa

entrada = c.empresas

conexao = bd.ConectionFactory()
con = conexao.getConnection()
cursor = con.cursor()

Table = Empresa.Empresas()
colunas = [
    "Cnpj_Basico",
    "Razao_Social",
    "Natureza_Juridica",
    "Qualificacao_Responsavel",
    "Capital_Social",
    "Porte_Social",
    "Ente_Federativo"
]

buffer = []
chunkSize = 20000

with (open(entrada, 'r', encoding="latin1") as arquivo):
    for linha in arquivo:
        linha = linha.strip()
        linha = linha.replace('"', "")
        campos = linha.split(";")

        if len(campos) < len(colunas):
            campos += [None] * (len(colunas) - len(campos))
        elif len(campos) > len(colunas):
            campos = campos[:len(colunas)]
        if len(campos) != len(colunas):
            print(f"Erro na linha: {campos} (esperado {len(colunas)}, obtido {len(campos)})")

        if campos[6] == '' or campos[6] is None:
            campos[6] = '0'

        buffer.append(campos)

        if len(buffer) >= chunkSize:
            chunk = pd.DataFrame(buffer, columns=colunas)
            chunk = chunk.replace(r'^\s*$', value=None, regex=True)

            data = []

            for _, row in chunk.iterrows():
                row_data = [None if pd.isna(val) else val for val in row]
                data.append(tuple(row_data))
            try:
                Table.incert(data)
                print(f"Chunk inserido com sucesso: {len(data)} registros")
            except Exception as e:
                print(f"Erro ao inserrir chunk {e}")
                con.rollback()
    buffer = []

if buffer:
    chunk = pd.DataFrame(buffer, columns=colunas)
    chunk = chunk.replace(r'^\s*$', value=None, regex=True)

data = []

for _, row in chunk.iterrows():
    row_data = [None if pd.isna(val) else val for val in row]
    data.append(tuple(row_data))

try:
    Table.insert(data)
    print(f"Chunk inserido com sucesso: {len(data)} registros")
except Exception as e:
    print(f"Erro ao inserrir chunk {e}")

buffer = []

cursor.close()
con.close()
print("Processamento Concluido")
