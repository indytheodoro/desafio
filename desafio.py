# -*- coding: utf-8 -*- 

import MySQLdb
import re
from datetime import datetime

def etllog():
    
    arq = open("../log/access_log_Aug95", "r")
    data = {}
    blocks = []
    
    # Conectando com o banco local
    connection = MySQLdb.connect(user='root')
    cursor = connection.cursor()  
    
    # Criando DB e table
    cursor.execute("CREATE DATABASE IF NOT EXISTS `kendoo`;")
    
    # Utilizando o banco criado
    connection = MySQLdb.connect(user='root', database='kendoo')
    cursor = connection.cursor()  
    cursor.execute("DROP TABLE IF EXISTS `desafio_ingrhid`;")
    
    create = """CREATE TABLE `desafio_ingrhid` (
    `id` BIGINT(20) NOT NULL AUTO_INCREMENT,
    `status` VARCHAR(10) NULL,
    `bytes` BIGINT(20) NULL,
    `datetime` DATETIME NULL,
    `host` VARCHAR(100) NULL,
    `path` VARCHAR(200) NULL,
    `method` VARCHAR(10) NULL,
    PRIMARY KEY (`id`));"""
                        
    cursor.execute(create)
    
    # Contadores
    i = 0
    bloco = 0
    tamanho_bloco = 10000
    # Tratamento dos dados
    for line in arq.readlines():
        i += 1
        pattern = re.compile(r'(.*) (.*) (.*) \[(.*)\] \"(GET|POST|HEAD|PUT|PATCH|DELETE|TRACE|)(.*)\" (.*) (.*)')
        match = pattern.match(line)
        if match:
            host, _, _, date, method, path, status, bytes = match.groups()
            bytes = bytes.replace("-","0")
            date = date[:11]
            date_corr = datetime.strptime(date, "%d/%b/%Y").date().isoformat()
        
            # Dados tratados
            data = {
                "host" : host,
                "datetime" : date_corr,
                "path" : path,
                "method": method,
                "status" : status,
                "bytes" : bytes
                } 
            blocks.append(data)
            if len(blocks) == tamanho_bloco:
                insert = "INSERT INTO `kendoo`.`desafio_ingrhid` (`status`, `bytes`, `datetime`, `host`, `path`, `method`) VALUES (%(status)s,%(bytes)s,%(datetime)s,%(host)s,%(path)s,%(method)s)"
                bloco += 1
                print "Inserindo dados: %s até %s." % ((bloco - 1) * tamanho_bloco + 1, bloco * tamanho_bloco)
                cursor.executemany(insert, blocks)
                blocks = []
            
    if blocks:
        insert = "INSERT INTO `kendoo`.`desafio_ingrhid` (`status`, `bytes`, `datetime`, `host`, `path`, `method`) VALUES (%(status)s,%(bytes)s,%(datetime)s,%(host)s,%(path)s,%(method)s)"
        cursor.executemany(insert, blocks)
        print "Inserindo últimos dados."
        
    # Fechando arquivo e conexão
    connection.commit()
    arq.close()
    cursor.close()
    connection.close()
    print "#### Finalizando processamento, as queries serão executadas. ####"
    
def queriesDB():
    # Iniciando a conexão com o banco
    connection = MySQLdb.connect(user='root', database='kendoo')
    cursor = connection.cursor()    
    
    # Definições das Queries
    m_bytes = "SELECT avg(bytes) FROM desafio_ingrhid;"
    p_resposta = "SELECT status, count(*) * 100 / (select count(*) FROM desafio_ingrhid) as percentual FROM desafio_ingrhid WHERE status BETWEEN 400 AND 600 GROUP BY status;"
    t_requisicoes = "SELECT count(1) FROM desafio_ingrhid WHERE datetime = '1995-08-06' ORDER BY count(1) desc LIMIT 10;"
    path_requisitados = "SELECT path, count(1) FROM desafio_ingrhid GROUP BY path ORDER BY count(1) desc LIMIT 10;"

    # Executando Queries
    export = open("./resultados.txt", "w")
    cursor.execute(m_bytes)
    export.write("Pergunta 1: Qual foi a média de bytes transferidos por requisição?\n")
    for avg_bytes in cursor:
        export.write("Média de bytes: %s.\n\n" % avg_bytes)
    cursor.execute(p_resposta)
    export.write("Pergunta 2: Qual o % de resposta com cada um dos códigos de erro?\n")
    for status, n in cursor:
         export.write("Respostas com status %s:  %s%%.\n" % (status, n))
    cursor.execute(t_requisicoes)
    export.write("\nPergunta 3: No dia 06 de Agosto  qual o total de requisições feitas?\n")
    for requisicoes in cursor:
        export.write("Dia 06/08/1995 houve %s requisições.\n\n" % (requisicoes))
    cursor.execute(path_requisitados)
    export.write("Pergunta 4: Quais foram os 10 paths mais requisitados e quantas requisições foram feitas para cada path?\n")
    header = "%2s|%-50s|%-10s\n%s\n" % ("#", "Path", "Requisições", "-" * 64)
    export.write(header)
    i = 0
    for path, n in cursor:
        i += 1
        line = "%2d|%-50s|%-10d\n" % (i, path, n)
        export.write(line)
        
    # Encerrando a conexão com o banco
    connection.commit()
    cursor.close()
    connection.close()
    print "#### Execução das Queries finalizada, consulte o arquivo resultados.txt ####"
    
    
if __name__ == "__main__":
    # Executa o tratamento e inserção dos dados no banco, irá printar quantos arquivos estão sendo inseridos no banco 
    # Uma vez que o banco estiver preenchido e queira executar apenas as queries, comentar a linha etllog()
    etllog()
    
    # Executa queries solitadas, exportando as repsostas para o arquivo resultados.txt 
    queriesDB()
