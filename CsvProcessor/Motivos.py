import ConectionFactory
import pandas as pd
import Config as c
import ConectionFactory as bd

entrada = c.motivos

colunas = ["Id", "Motivos"]
buffer = []
chunkSize = 100

connection = bd.ConectionFactory()
con = connection.getConnection()
cursor = con.cursor()

with open(entrada, 'r', encoding='latin1') as arquivo:
    for linha in arquivo:
        # Corrigir: aplicar as transformações corretamente
        linha = linha.strip()
        linha = linha.replace('"', '')  # ✅ Atribuir o resultado
        campos = linha.split(";")  # ✅ Atribuir o resultado à variável campos

        # Ajustar número de campos
        if len(campos) < len(colunas):
            campos += [None] * (len(colunas) - len(campos))
        elif len(campos) > len(colunas):
            campos = campos[:len(colunas)]

        buffer.append(campos)  # ✅ Adicionar a lista de campos, não a string

        if len(buffer) >= chunkSize:
            chunk = pd.DataFrame(buffer, columns=colunas)
            chunk = chunk.replace(r'^\s*$', None, regex=True)

            data = []
            for _, row in chunk.iterrows():
                # Converte valores pandas NA para None (NULL no SQL)
                row_data = [None if pd.isna(val) else val for val in row]
                data.append(tuple(row_data))

            placeHolders = ", ".join(['?'] * len(colunas))
            # Corrigir: SQL com sintaxe correta
            sql = f"INSERT INTO Motivos ({', '.join(colunas)}) VALUES ({placeHolders})"

            try:
                cursor.executemany(sql, data)
                con.commit()
                print(f"Chunk inserido com sucesso: {len(data)} registros")
            except Exception as e:
                print(f"Erro ao inserir chunk: {e}")
                con.rollback()

            buffer = []

# Processar buffer restante (se houver)
if buffer:
    chunk = pd.DataFrame(buffer, columns=colunas)
    chunk = chunk.replace(r'^\s*$', None, regex=True)

    data = []
    for _, row in chunk.iterrows():
        row_data = [None if pd.isna(val) else val for val in row]
        data.append(tuple(row_data))

    placeHolders = ", ".join(['?'] * len(colunas))
    sql = f"INSERT INTO Motivos ({', '.join(colunas)}) VALUES ({placeHolders})"

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