import Config as c
import ConnectionFactory as bd

class Estabelecimentos():
    colunas = [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
        'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior',
        'pais', 'data_inicio_atividade', 'cnae_principal', 'cnae_secundario', 'tipo_logradouro', 'logradouro',
        'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
        'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial'
    ]
    con =  bd.ConectionFactory().getConnection()
    def __init__(self):
        con = bd.ConectionFactory().getConnection()


    def Create(self):
        sql = (
           "CREATE TABLE [dbo].[Estabelecimentos]("
            "[cnpj_basico] [varchar](80) NULL,"
            "[cnpj_ordem] [varchar](40) NULL,"
            "[cnpj_dv] [varchar](200) NULL,"
            "[identificador_matriz_filial] [varchar](100) NULL,"
            "[nome_fantasia] [varchar](100) NULL,"
            "[situacao_cadastral] [varchar](200) NULL,"
            "[data_situacao_cadastral] [date] NULL,"
            "[motivo_situacao_cadastral] [varchar](200) NULL,"
            "[nome_cidade_exterior] [varchar](100) NULL,"
            "[pais] [varchar](30) NULL,"
            "[data_inicio_atividade] [date] NULL,"
            "[cnae_principal] [varchar](70) NULL,"
            "[cnae_secundario] [varchar](1000) NULL,"
            "[tipo_logradouro] [varchar](200) NULL,"
            "[logradouro] [varchar](100) NULL,"
            "[numero] [varchar](100) NULL,"
            "[complemento] [varchar](200) NULL,"
            "[bairro] [varchar](200) NULL,"
            "[cep] [varchar](100) NULL,"
            "[uf] [varchar](1000) NULL,"
            "[municipio] [varchar](400) NULL,"
            "[ddd_1] [varchar](400) NULL,"
            "[telefone_1] [varchar](900) NULL,"
            "[ddd_2] [varchar](400) NULL,"
            "[telefone_2] [varchar](900) NULL,"
            "[ddd_fax] [varchar](400) NULL,"
           " [fax] [varchar](900) NULL,"
            "[correio_eletronico] [varchar](200) NULL,"
            "[situacao_especial] [varchar](100) NULL,"
            "[data_situacao_especial] [date] NULL,"
            "[cnpj] [varchar](140) NULL)"
        )
        self.con.execute(sql)

    def insert(self, data):
        placeholders = ', '.join(['?'] * len(self.colunas))
        sql = f"INSERT INTO Estabelecimentos ({', '.join(self.colunas)}) VALUES ({placeholders})"
        self.con.cursor().executemany(sql, data)
        self.con.commit()