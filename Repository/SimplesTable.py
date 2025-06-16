import ConnectionFactory as bd

class Simples():
    colunas = ["CnpjBasico", "OpSimples", "DataOpSimples", "DataExclusaoSimples",
               "OpMei", "DatapeloMei", "DataExclusaoMei"]
    conexao = bd.ConectionFactory()

    def __int__(self):
        colunas = ["CnpjBasico", "OpSimples", "DataOpSimples", "DataExclusaoSimples",
                   "OpMei", "DatapeloMei", "DataExclusaoMei"]

    def create(self):
        sql = (f"CREATE TABLE IF NOT EXIST Simples ("
               f"[CnpjBasico] VARCHAR(200) NULL,"
               f"[Opsimples] VARCHAR(50) NULL,"
               f"[DataOpSimples] DATE NULL,"
               f"[DataExclusaoSimples] DATE NULL,"
               f"[OpMei] VARCHAR(100) NULL,"
               f"[DatapeloMei] DATE NULL ,"
               f"[DataExclusaoMei] DATE NULL"
               f")"
               )
        try:
            self.conexao.getConnection().execute(sql)
            self.conexao.getConnection().commit()
        except Exception as e:
            print(f"Ocorreu um erro ao executar o código  no banco de dados {e}")
            self.conexao.getConnection().rollback()

    def insert(self,data):
        sql = f"INSERT INTO Simples ({', '.join(self.colunas)}) VALUES(?,?,?,?,?,?,?)"

        try:
            self.conexao.GetCursor().executemany(sql,data)
            self.conexao.getConnection().commit()
        except Exception as e:
            print(f"Ocorreu um erro ao inserir no banco de dados {e}")
            self.conexao.getConnection().rollback()
