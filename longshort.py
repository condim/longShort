import psycopg2, pandas, numpy as np

conn = psycopg2.connect(user = "postgres",
                              password = "condim",
                              host = "localhost",
                              port = "5432")
conn.autocommit = True
cur = conn.cursor()
cur.execute('DROP DATABASE bmf')
cur.execute('CREATE DATABASE bmf')
conn.commit()
conn.close()

def bdPandas(n):
    conn = psycopg2.connect(user = "postgres",
                              password = "condim",
                              host = "localhost",
                              port = "5432",
                              database = "bmf")
    correlation1 = pandas.read_sql('select * from corr limit '+str(n)+';',conn)
    correlation=correlation1.corr().abs()

    correlation2 = (correlation.where(np.triu(np.ones(correlation.shape), k=1).astype(np.bool))
           .stack()
           .sort_values(ascending=False)).head(5)
    # first element of sol series is the pair with the bigest correlation
    return correlation2

def bdInput(command):
    conn = psycopg2.connect(user = "postgres",
                              password = "condim",
                              host = "localhost",
                              port = "5432",
                              database = "bmf")
    cur = conn.cursor()
    cur.execute(command)
    conn.commit()
    conn.close()

def bdSelect(command):
    conn = psycopg2.connect(user = "postgres",
                              password = "condim",
                              host = "localhost",
                              port = "5432",
                              database = "bmf")
    cur = conn.cursor()
    cur.execute(command)
    record = cur.fetchall()
    return(record)
    conn.commit()
    conn.close()

arquivo = 'COTAHIST_A2019.TXT'
filtro1 = [' ','1','2','3','4','5','6','7','8','9','0']
listaAcao = ['PETR4','PETR3','VALE3','BBAS3','ITUB4','ABEV3','AZUL4','ANIM3',
             'BTOW3','BIDI3','BBDC3','BRPR3','BRKM3','BRSR3','ELET3','CESP3',
             'ELPL3','ENGI3','ECOR3','FJTA3','GFSA3','GGBR3','GOLL4','ITSA3',
             'LIGT3','LAME3','RENT3','GOAU3','MRVE3','ODPV3','OIBR3','RADL3',
             'ALPA3','SUZB3','TELB3','TOTS3','USIM3',]
bdInput('create table corr (id numeric primary key)')
def longShort(acao):
    print(acao)
    bdInput('create table '+acao+' ( id smallint,fechamento numeric,variacao numeric)')
    bdInput('create sequence '+acao+'_SEQ start with 1')
    with open(arquivo) as file:
        for line in file:
            if len(acao) == 5 and line[12:17] == acao and line[17] == ' ':
                bdInput("insert into "+acao+" (id,fechamento) values (nextval('"+acao+"_SEQ'),'"+str(float(line[109:121])/100)+"');")
            elif len(acao) == 6 and line[12:17] == acao[0:5] and acao[4] in filtro1:
                bdInput("insert into "+acao+" (id,fechamento) values (nextval('"+acao+"_SEQ'),'"+str(float(line[109:121])/100)+"');")
    last = int(bdSelect("SELECT max(id) from "+acao+";")[0][0])
    bdInput("alter table corr add column "+acao.lower()+" numeric;")
    for indice in range(1,last):
        valor1=float(bdSelect('select fechamento from '+acao+' where id ='+str(indice)+';')[0][0])
        valor2=float(bdSelect('select fechamento from '+acao+' where id >'+str(indice)+';')[0][0])
        vari=str((valor1/valor2)-1)
        bdInput('insert into corr (id,'+acao+') values ('+str(indice)+','+vari+') on conflict on constraint corr_pkey do update set '+acao+' = '+vari+';')
print(list(map(longShort, listaAcao)))
bdInput('alter table corr drop constraint corr_pkey;')
bdInput('alter table corr drop column id')
print("correlacao de 247 periodos: \n"+str(bdPandas(247))+'\n')
print("correlacao de 180 periodos: \n"+str(bdPandas(180))+'\n')
print("correlacao de 90 periodos: \n"+str(bdPandas(90))+'\n')
print("correlacao de 45 periodos: \n"+str(bdPandas(45))+'\n')


