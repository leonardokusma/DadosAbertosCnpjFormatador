import re
import pandas as pd
import pyodbc as db
import ConectionFactory as c


def limpar_linha(line):
    # Remove aspas duplas e mantém o restante
    line = line.strip()
    return line.replace('"', '')


def analisar_estrutura_arquivo(caminho_arquivo, num_linhas=10):

    print("=== ANÁLISE DA ESTRUTURA DO ARQUIVO ===")

    with open(caminho_arquivo, 'r', encoding='latin1') as arquivo:
        for i, linha in enumerate(arquivo):
            if i >= num_linhas:
                break

            limpa = limpar_linha(linha)
            valores = limpa.split(';')

            print(f"\nLinha {i + 1}:")
            print(f"Número de colunas: {len(valores)}")

            # Mostra os primeiros valores para identificação
            for j, valor in enumerate(valores[:15]):  # Mostra apenas as primeiras 15 colunas
                print(f"  Col {j + 1:2d}: '{valor}'")

            if len(valores) > 15:
                print(f"  ... e mais {len(valores) - 15} colunas")


def validar_mapeamento_colunas(caminho_arquivo, colunas_esperadas, num_linhas=5):
    """Valida se o mapeamento das colunas está correto"""
    print("\n=== VALIDAÇÃO DO MAPEAMENTO ===")

    with open(caminho_arquivo, 'r', encoding='latin1') as arquivo:
        for i, linha in enumerate(arquivo):
            if i >= num_linhas:
                break

            limpa = limpar_linha(linha)
            valores = limpa.split(';')

            print(f"\nLinha {i + 1} - Mapeamento sugerido:")

            for j, (nome_col, valor) in enumerate(zip(colunas_esperadas, valores)):
                # Destaca possíveis problemas
                status = ""
                if nome_col in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv'] and not valor.isdigit():
                    status = " ⚠️  (Esperado: número)"
                elif 'data_' in nome_col and not re.match(r'\d{8}', valor):
                    status = " ⚠️  (Esperado: data YYYYMMDD)"
                elif nome_col == 'uf' and len(valor) != 2:
                    status = " ⚠️  (Esperado: UF com 2 caracteres)"

                print(f"  {nome_col:25s}: '{valor}'{status}")

            if len(valores) > len(colunas_esperadas):
                print(f"  ⚠️  ATENÇÃO: {len(valores) - len(colunas_esperadas)} colunas extras encontradas!")


entrada = r'D:\Eng. de software\5 Período\jornada\DadosCnpj\K3241.K03200Y0.D50510.ESTABELE'


colunas = [
    'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
    'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior',
    'pais', 'data_inicio_atividade', 'cnae_principal', 'cnae_secundario', 'tipo_logradouro', 'logradouro',
    'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
    'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial'
]


colunas_numericas = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax',
                     'fax', 'cep']



# Conexão com o banco de dados
connection = c.ConectionFactory()
con = connection.getConnection()
cursor = con.cursor()

chunk_size = 1000
buffer = []

with open(entrada, 'r', encoding='latin1') as arquivo:
    for linha in arquivo:
        limpa = limpar_linha(linha)
        valores = limpa.split(';')

        if len(valores) < len(colunas):
            valores += [None] * (len(colunas) - len(valores))
        elif len(valores) > len(colunas):
            valores = valores[:len(colunas)]

        buffer.append(valores)

        # Quando atingir o tamanho do chunk, processa
        if len(buffer) >= chunk_size:
            chunk = pd.DataFrame(buffer, columns=colunas)

            # Substitui strings vazias e espaços por None
            chunk = chunk.replace(r'^\s*$', None, regex=True)

            # Converte datas
            for col in ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial']:
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], format='%Y%m%d', errors='coerce')
                    chunk[col] = chunk[col].dt.strftime('%Y-%m-%d')
                    chunk[col] = chunk[col].replace('NaT', None)

            # Tratamento melhorado para colunas numéricas
            for col in colunas_numericas:
                if col in chunk.columns:
                    # Remove espaços em branco e converte strings vazias para None
                    chunk[col] = chunk[col].astype(str).str.strip()
                    chunk[col] = chunk[col].replace(['', 'nan', 'None'], None)

                    # Converte para numérico
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

                    # Substitui NaN por 0 (ou mantenha None se preferir NULL no banco)
                    chunk[col] = chunk[col].fillna(0).astype('Int64')  # Usa Int64 para permitir NaN

            data = []
            for _, row in chunk.iterrows():
                row_data = [None if pd.isna(val) else val for val in row]
                data.append(tuple(row_data))

            placeholders = ', '.join(['?'] * len(colunas))
            sql = f"INSERT INTO Estabelecimentos ({', '.join(colunas)}) VALUES ({placeholders})"

            try:
                cursor.executemany(sql, data)
                con.commit()
                print(f"Chunk inserido com sucesso: {len(data)} registros")
            except Exception as e:
                print(f"Erro ao inserir chunk: {e}")
                con.rollback()

            buffer = []

if buffer:
    chunk = pd.DataFrame(buffer, columns=colunas)

cursor.close()
con.close()
print("Processamento concluído!")
