import Config as c
import ConectionFactory as bd

class Empresas(bd):
    colunas = [
        "Cnpj_Basico",
        "Razao_Social",
        "Natureza_Juridica",
        "Qualificacao_Responsavel",
        "Capital_Social",
        "Porte_Social",
        "Ente_Federativo"
    ]
    con =  bd.ConectionFactory().getConnection()
    def __init__(self):
        con = bd.ConectionFactory().getConnection()


    def Create(self):
        sql = (
            "CREATE TABLE Empresas ("
            " Cnpj_Basico              VARCHAR(100) NULL,"
            " Razao_Social             VARCHAR(1000) NULL,"
            " Natureza_Juridica        VARCHAR(MAX) NULL,"
            " Qualificacao_Responsavel VARCHAR(100) NULL,"
            " Capital_Social           INT NULL,"
            " Porte_Social             VARCHAR(600) NULL,"
            " Ente_Federativo          VARCHAR(700) NULL"
            ");"
        )
        self.con.execute(sql)

    def incert(self, data):
        sql = f"INSERT INTO Empresas ({', '.join(self.colunas)}) VALUES (?,?,?,?,?,?,?)"
        self.con.cursor().executemany(sql, data)
        self.con.commit()