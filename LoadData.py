import sqlalchemy
import sqlite3
import pandas as pd


class LoadData:

    def __init__(self , database,nome_banco,  dados ):

        self.engine = sqlalchemy.create_engine(database)
        self.dados = dados
        self.nome_banco = nome_banco

    def __criando_conexao(self):

        conn = sqlite3.connect(self.nome_banco)


        return conn


    def criando_table(self, query_table):

        conexao = self.__criando_conexao()
        cursor = conexao.cursor()
        cursor.execute(query_table)
        cursor.close()

        return print("Tabela de dados criada")


    def inserindo_dados(self, tabela, id_tabela):

        self.__tabela_temporaria(tabela)

        conexao = self.__criando_conexao()
        cursor = conexao.cursor()

        cursor.execute(
                f"""
                        INSERT INTO artistas
                        SELECT tmp_table.* FROM tmp_table
                        LEFT JOIN artistas USING (id_artista)
                        WHERE artistas.id_artista IS NULL;

                        """)
        conexao.commit()


    def __tabela_temporaria(self, tabela):


        sql_query = f""" CREATE TEMP TABLE IF NOT EXISTS 
                        tmp_table AS SELECT * FROM {tabela} LIMIT 0;
                        
                        """

        cursor  = self.__criando_conexao()

        cursor.executescript(sql_query)

        self.dados.to_sql("tmp_table", con = self.engine , if_exists='append', index = False)

        cursor.close()


        return True

