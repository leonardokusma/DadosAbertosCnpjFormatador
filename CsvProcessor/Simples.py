import pandas as pd
import ConectionFactory as bd
import Config as c


entrada = c.simples

conexaoFactory = bd.ConectionFactory()
con = conexaoFactory.getConnection()
cursor = con.cursor()

colunas = ["CnpjBasico", "OpSimples", "DataOpSimples", "DataExclusaoSimples",
           "OpMei", "DatapeloMei", "DataExclusaoMei"]

buffer = []
chunkSize = 1000

with (open(entrada, 'r', encoding="latin1") as arquivo):
    for linha in arquivo:
        linha = linha.strip()
        linha = linha.replace('""', '0')
        linha = linha.replace('"',"")
        campos = linha.split(";")

        if len(campos) < len(colunas):
            campos += [None] * (len(colunas) - len(campos))
        elif len(campos) > len(colunas):
            campos = campos[:len(colunas)]

        buffer.append(campos)

        if len(buffer) >= chunkSize:
            chunk = pd.DataFrame(buffer, columns= colunas)
            chunk = chunk.replace(r'^\s*$', value=None, regex=True)

            for col in ["DataOpimples", "DataExclusaoSimples","DatapeloMei", "DataExclusaoMei"]:
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], format='%Y%m%d', errors='coerce')
                    chunk[col] = chunk[col].dt.strftime('%Y-%m-%d')
                    chunk[col] = chunk[col].replace('NaT', None)

            data = []
            for _, row in chunk.iterrows():
                row_data = [None if pd.isna(val) else val for val in row]
                data.append(tuple(row_data))

            placeHolders = ', '.join(['?'] * len(colunas))
            sql = f"INSERT INTO Simples ({', '.join(colunas)}) VALUES ({placeHolders})"

            try:
                cursor.executemany(sql,data)
                con.commit()

                print(f"Chunk inserido com sucesso: {len(data)} registros")

            except Exception as e:
                print(f"Erro ao inserir chunk {e}")
                con.rollback()

            buffer = []

cursor.close()
con.close()
print("Processamento Concluído!")