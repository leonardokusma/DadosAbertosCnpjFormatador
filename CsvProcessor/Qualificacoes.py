import Config as c
import ConectionFactory as bd
import pandas as pd

entrada = c.qualificacoes

conexao = bd.ConectionFactory()
con = conexao.getConnection()
cursor = con.cursor()

colunas = ["Codigo", "Descricao"]

buffer = []

chunkSize = 100

with open(entrada, 'r', encoding="cp1252") as arquivo:
    for linha in arquivo:
        linha = linha.strip()
        linha = linha.replace('"', "")
        campos = linha.split(";")

        if len(campos) < len(colunas):
            campos += [None] * (len(colunas) - len(campos))
        elif len(campos) > len(colunas):
            campos = campos[:len(colunas)]

        buffer.append(campos)

        if len(buffer) >= chunkSize:
            chunk = pd.DataFrame(buffer, columns=colunas)
            chunk = chunk.replace(r'^\s*$', value=None, regex=True)

            data = []
            for _, row in chunk.iterrows():
                row_data = [None if pd.isna(val) else val for val in row]
                data.append(tuple(row_data))

            placeHolders = ', '.join(['?'] * len(colunas))
            sql = f"INSERT INTO Qualificacoes ({', '.join(colunas)}) VALUES ({placeHolders})"

            try:
                cursor.executemany(sql,data)
                con.commit()
                print(f"Chunk inserido com sucesso: {len(data)} registros")
            except Exception as e:
                print(f"Erro ao inserido chunk: {e}")
                con.rollback()

            buffer = []

if buffer:
    chunk = pd.DataFrame(buffer, columns=colunas)
    chunk = chunk.replace(r'^\s*$', value=None, regex=True)

    data = []
    for _, row in chunk.iterrows():
        row_data = [None if pd.isna(val) else val for val in row]
        data.append(tuple(row_data))

    placeHolders = ', '.join(['?'] * len(colunas))
    sql = f"INSERT INTO Qualificacoes ({', '.join(colunas)}) VALUES ({placeHolders})"

    try:
        cursor.executemany(sql, data)
        con.commit()
        print(f"Chunk final inserido com sucesso: {len(data)} registros")
    except Exception as e:
        print(f"Erro ao inserir chunk final: {e}")
        con.rollback()

cursor.close()
con.close()
print("Processamento concluído!")
