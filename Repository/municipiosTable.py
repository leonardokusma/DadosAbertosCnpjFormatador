import Config as c
import ConnectionFactory as bd


class Municipios():
    colunas = []
    conexao = bd.ConectionFactory()

    def __init__(self):
        colunas = ["Codigo", "Descricao"]

    def Create(self):
        sql = ("CREATE TABLE IF NOT EXIST Municipios ("
               "[Codigo] INT NOT NULL,"
               "[Descricao] Varchar (600)"
               ")")
        try:
            self.conexao.GetCursor().execute(sql)
            self.conexao.getConnection().commit()
        except Exception as e:
            print(f"Aconteceu um Erro ao executar o código De create table {e}")
            self.conexao.getConnection().rollback()

    def inser(self, data):
        sql = f"INSERT INTO Municipios ({','.join(self.colunas)}) VALUES(?,?)"
        try:
            self.conexao.GetCursor().executemany(sql, data)
            self.conexao.getConnection().commit()
        except Exception as e:
            print(f"Aconteceu um erro ao inserir no banco de dados {e}")
            self.conexao.getConnection().rollback()
