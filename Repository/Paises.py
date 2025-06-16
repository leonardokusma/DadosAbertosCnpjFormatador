import ConnectionFactory as bd


class Paises():
    colunas = []
    conexao = bd.ConectionFactory()

    def __init__(self):
        colunas = ["Codigo", "Descricao"]

    def create(self):
        sql = (f"CREATE TABLE IF NOT EXISTS Paises("
               f"[codigo] INT NULL,"
               f"[Descricao] varchar(700)"
               f")")
        try:
            self.conexao.getConnection().execute(sql)
        except Exception as e:
            print(f"Ocorreu um erro ao executar o código de create {e}")

    def insert(self, data):
        sql = f"INSERT INTO Paises ({', '.join(self.colunas)}) VALUES (?,?)"

        try:
            self.conexao.GetCursor().executemany(sql, data)
            self.conexao.getConnection().commit()
        except Exception as e:
            print(f"Ocorreu um erro ao inserir no banco {e}")
            self.conexao.getConnection().rollback()
