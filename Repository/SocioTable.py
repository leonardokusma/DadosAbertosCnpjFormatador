import ConnectionFactory as bd


class Simples():
    colunas = ["CnpjBasico", "IdentificadorSocio", "NomeSocio", "CnpjSocio", "QualifSocio", "DataEntradaSocie", "Pais",
               "RepresentanteLegal", "NomeRepresentanteLegal", "CodQualiRepresentante", "FaixaEtaria"]
    conexao = bd.ConectionFactory()

    def __int__(self):
        colunas = ["CnpjBasico", "IdentificadorSocio", "NomeSocio", "CnpjSocio", "QualifSocio", "DataEntradaSocie",
                   "Pais",
                   "RepresentanteLegal", "NomeRepresentanteLegal", "CodQualiRepresentante", "FaixaEtaria"]
    def create(self):
        sql = (f"CREATE TABLE IF NOT EXIST Simples ("
               f"[CnpjBasico] VARCHAR(200) NULL,"
               f"[IdentificadorSocio] INT NULL,"
               f"[NomeSocio] VARCHAR(300) NULL,"
               f"[CnpjSocio] VARCHAR(300)NULL,"
               f"[QualifSocio] INT NULL,"
               f"[DataEntradaSocie] DATE NULL ,"
               f"[Pais] INT NULL,"
               f"[RepresentanteLegal] VARCHAR(100) NULL,"
               f"[NomeRepresentanteLegal] VARCHAR(300) NULL,"
               f"[CodQualiRepresentante] INT NULL,"
               f"[FaixaEtaria] INT NULL"
               f")"
               )
        try:
            self.conexao.getConnection().execute(sql)
            self.conexao.getConnection().commit()
        except Exception as e:
            print(f"Ocorreu um erro ao executar o código  no banco de dados {e}")
            self.conexao.getConnection().rollback()

    def insert(self,data):
        sql = f"INSERT INTO Simples ({', '.join(self.colunas)}) VALUES(?,?,?,?,?,?,?,?,?,?,?)"

        try:
            self.conexao.GetCursor().executemany(sql,data)
            self.conexao.getConnection().commit()
        except Exception as e:
            print(f"Ocorreu um erro ao inserir no banco de dados {e}")
            self.conexao.getConnection().rollback()
