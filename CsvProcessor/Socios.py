import ConectionFactory as bd
import pandas as pd
import Config as c

entrada = c.socios

colunas = ["CnpjBasico", "IdentificadorSocio", "NomeSocio", "CnpjSocio", "QualifSocio", "DataEntradaSocie", "Pais",
           "RepresentanteLegal", "NomeRepresentanteLegal", "CodQualiRepresentante", "FaixaEtaria"]

buffer = []
chunkSize = 1000

connection = bd.ConectionFactory()
con = connection.getConnection()
cursor = con.cursor()

with open(entrada, 'r', encoding="latin1") as arquivo:
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

            for col in  ['DataEntradaSocie']:
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], format='%Y%m%d', errors='coerce')
                    chunk[col] = chunk[col].dt.strftime('%Y-%m-%d')
                    chunk[col] = chunk[col].replace('NaT', None)

            data = []
            for _, row in chunk.iterrows():
                row_data = [None if pd.isna(val) else val for val in row]
                data.append(tuple(row_data))

            placeHolders = ', '.join(['?'] * len(colunas))
            sql = f"INSERT INTO SOCIO ({', '.join(colunas)}) VALUES ({placeHolders})"

            try:
                cursor.executemany(sql, data)
                con.commit()
                print(f"Chunk inserido com sucesso: {len(data)} registros")
            except Exception as e:
                print(f"Erro ao inserir chunck: {e}")
                con.rollback()

            buffer = []

if buffer:
    # Repete o mesmo processo para o buffer final
    chunk = pd.DataFrame(buffer, columns=colunas)
    # ... (mesmo código de processamento)

cursor.close()
con.close()
print("Processamento concluído!")
