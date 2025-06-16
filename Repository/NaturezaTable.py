import ConnectionFactory as bd

class Natureza():
    colunas = []
    conexao = bd.ConectionFactory()
    def __init__(self):
        colunas = ["Codigo", "Descricao"]


    def create(self):
        sql = (f"CREATE TABLE IF NOT EXIST Natureza ("
               f"[codigo] INT NOT NULL,"
               "[Descricao] varchar(600)"
               ")")
        try:
            self.conexao.getConnection().execute(sql)
            self.conexao.getConnection().commit()
        except Exception as e:
            print(f"Ocorreu um erro ao inserir no banco {e}")
            self.conexao.getConnection().rollback()

    def insert(self,data):
        sql = f"INSERT INTO NATUREZA ({', '.join(self.colunas)}) VALUES(?,?)"
        try:
            self.conexao.GetCursor().executemany(sql,data)
            self.conexao.getConnection().commit()
        except Exception as e:
            print(f"Ocorreu um erro ao inserir no banco {e}")
            self.conexao.getConnection().rollback()
