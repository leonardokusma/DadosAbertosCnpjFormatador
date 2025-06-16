import ConnectionFactory as bd

class Qualificacoes():
    colunas = []
    conexao = bd.ConectionFactory()
    def __int__(self):
        colunas = ["Codigo", "Descricao"]

    def create(self):
        sql = (f"CREATE TABLE IF NOT EXIST Qualificacoes ("
               f"[Codigo] INT NULL,"
               f"[Descricao] VARCHAR(900)"
               f")")
        try:
            self.conexao.getConnection().execute(sql)
            self.conexao.getConnection().commit()
        except Exception as e:
            print(f"Ocorreu um erro ao executar o código de create {e}")
            self.conexao.getConnection().rollback()

    def insert(self,data):
        sql = f"INSER INTO Qualificacoes ({', '.join(self.colunas)}) VALUES(?,?)"

        try:
            self.conexao.GetCursor().executemany(sql,data)
            self.conexao.getConnection().commit()

        except Exception as e:
            print(f"Ocorreu um erro ao inserir no banco de dados: {e}")
            self.conexao.getConnection().rollback()