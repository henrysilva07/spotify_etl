import sqlalchemy
import sqlite3



class LoadData:

    def __init__(self, database, nome_banco, dados):

        self.engine = sqlalchemy.create_engine(database)
        self.dados = dados
        self.nome_banco = nome_banco

    def __criando_conexao(self):

        conn = sqlite3.connect(self.nome_banco)

        return conn

    @staticmethod
    def __executando_query(conn, query):

        cursor = conn.cursor()
        cursor.executescript(query)

        return cursor


    def criando_table(self, query_table):

        conexao = self.__criando_conexao()
        cursor = self.__executando_query(conexao, query_table)
        cursor.close()
        conexao.commit()

        return print("Tabela de dados criada")


    def inserindo_dados(self, tabela, id_tabela):

        self.__tabela_temporaria(tabela)

        conexao = self.__criando_conexao()


        query = f"""
                        INSERT INTO {tabela}
                        SELECT tmp_table.* FROM tmp_table
                        LEFT JOIN {tabela} USING ({id_tabela})
                        WHERE {tabela}.{id_tabela} IS NULL;
                        
                        DROP TABLE tmp_table;

                        """
        self.__executando_query(conexao, query)
        conexao.commit()


    def __tabela_temporaria(self, tabela):


        query = f""" CREATE TEMP TABLE IF NOT EXISTS 
                        tmp_table AS SELECT * FROM {tabela} LIMIT 0;
                        
                        """

        conexao = self.__criando_conexao()

        cursor = self.__executando_query(conexao, query)

        self.dados.to_sql("tmp_table", con=self.engine, if_exists='append', index=False)

        cursor.close()

        conexao.commit()

        return True
