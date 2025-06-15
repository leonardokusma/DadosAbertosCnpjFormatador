import ConectionFactory as bd


class MotivosTable:
    con = bd.ConectionFactory().getConnection()
    def __init__(self):
        cursor = self.con.GetCursor()


    def Create(self):

        sql = (f"CREATE TABLE IF NOT EXIST Motivos("
               f"Codigo INT NULL,"
               f"Descricao VARCHAR(600) NULL"
               f")")

        try:
            self.con.GetCursor().execute(sql)
        except Exception as e:
            print(f"O Erro {e} aconteceu")


    def insert(self,data):

        sql = f"INSERT INTO Motivos (Codigo, Descricao) VALUES (??)"
        try:
            self.con.GetCursor().executemany(sql,data)
            self.con.commit()
        except Exception as e:
            print(f"O Erro {e} aconteceu")
            self.con.rollback()