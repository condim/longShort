import psycopg2, pandas, numpy as np, urllib.request as download, zipfile, os, datetime
from concurrent.futures import ThreadPoolExecutor

#Download dos dados de todas as açoes da B3 do ultimo ano. Faz o download dos 12 meses anteriores,
#download dos dias anteriores do mes atual e descompacta
def dataDownload():
    hoje=datetime.datetime.now()
    mes=hoje.strftime('%m')
    ano=hoje.strftime('%Y')
    dia=hoje.strftime('%d')
    for i in range(1,13):
        if i < int(mes):
            download.urlretrieve('http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M'+str(i).zfill(2)+ano+'.ZIP','COTAHIST_'+ano+str(i).zfill(2)+'00'+'.ZIP')
            zipall=zipfile.ZipFile('COTAHIST_'+ano+str(i).zfill(2)+'00'+'.ZIP','r')
            zipintel=zipall.infolist()
            for zip in zipintel:
                zip.filename=ano+str(i).zfill(2)+'00'
                zipall.extract(zip)
        else:
            download.urlretrieve('http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M'+str(i).zfill(2)+str(int(ano)-1)+'.ZIP','COTAHIST_'+str(int(ano)-1)+str(i).zfill(2)+'00'+'.ZIP')
            zipall=zipfile.ZipFile('COTAHIST_'+str(int(ano)-1)+str(i).zfill(2)+'00'+'.ZIP','r')
            zipintel=zipall.infolist()
            for zip in zipintel:
                zip.filename=str(int(ano)-1)+str(i).zfill(2)+'00'
                zipall.extract(zip)
    for i in range(1,32):
        if i <= int(dia):
            try:
                download.urlretrieve('http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D'+str(i).zfill(2)+mes+ano+'.ZIP','COTAHIST_'+ano+mes+str(i).zfill(2)+'.ZIP')
                zipall=zipfile.ZipFile('COTAHIST_'+ano+mes+str(i).zfill(2)+'.ZIP','r')
                zipintel=zipall.infolist()
                for zip in zipintel:
                    zip.filename=ano+mes+str(i).zfill(2)
                    zipall.extract(zip)
            except:
                pass

#como o dataDownload gera muitos arquivos, essa funcao junta-os em 1 unico arquivo e deleta o restante
def dataCleaner():
    folder=os.listdir('.')
    list(folder)
    folder2=[]
    for file in folder:
        if file.isnumeric():
            folder2.append(file)
        elif file.endswith("ZIP"):
            os.remove(file)
    with open('historico.txt', 'w') as outfile:
        for fname in sorted(folder2):
            with open(fname) as infile:
                for line in infile:
                    outfile.write(line)
    for file in folder2:
        os.remove(file)

#reseta o banco de dados para novo calculo
def bdReset():
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
    bdInput('create table corr (id numeric primary key)')

#faz a correlaçao de todas açoes por todas as açoes inseridas e retorna as 10 maiores correlaçoes,
#o argumento n passado e o numero de periodos que devem ser buscados
def bdPandas(n):
    conn = psycopg2.connect(user = "postgres",
                              password = "condim",
                              host = "localhost",
                              port = "5432",
                              database = "bmf")
    correlation1 = pandas.read_sql('select * from corr order by id asc limit '+str(n)+';',conn,index_col='id')
    correlation=correlation1.corr().abs()
    correlation2 = (correlation.where(np.triu(np.ones(correlation.shape), k=1).astype(np.bool))
           .stack()
           .sort_values(ascending=False)).head(10).to_string()
    return correlation2

#Como o bdPandas(n) retorna as 10 melhores correlaçoes, essa funçao foi feita para buscar
#todas as correlaçoes de uma dada acao pelo numero n de periodos
def bdPandasSpec(spec,n):
    conn = psycopg2.connect(user = "postgres",
                              password = "condim",
                              host = "localhost",
                              port = "5432",
                              database = "bmf")
    correlation1 = pandas.read_sql('select * from corr order by id asc limit '+str(n)+';',conn,index_col='id')
    correlation2 = correlation1.corrwith(correlation1[spec]).sort_values(ascending=False).to_string()
    return correlation2

#funcao para inserir dados no banco de dados
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

#funcao para buscar dados no banco de dados
def bdSelect(command):
    conn = psycopg2.connect(user = "postgres",
                              password = "condim",
                              host = "localhost",
                              port = "5432",
                              database = "bmf")
    cur = conn.cursor()
    cur.execute(command)
    record = cur.fetchall()
    conn.close()
    return(record)

arquivo = 'historico.txt' #arquivo gerado pela funcao dataCleaner()
filtro1 = [' ','1','2','3','4','5','6','7','8','9','0'] #filtro usado num if da funcao longShort(), nao mexer
listaAcao = ['PETR4','PETR3','VALE3','BBAS3','ITUB4','ABEV3','AZUL4','ANIM3',
             'BTOW3','BIDI3','BBDC3','BRPR3','BRKM3','BRSR3','ELET3','CESP3',
             'ELPL3','ENGI3','ECOR3','FJTA3','GFSA3','GGBR3','GOLL4','ITSA3',
             'LIGT3','LAME3','RENT3','GOAU3','MRVE3','ODPV3','OIBR3','RADL3',
             'ALPA3','SUZB3','TELB3','TOTS3','USIM3','UNIP3','UGPA3','VVAR3',
             'TIET3','ALUP3','CIEL3','SBSP3','EMBR3','GRND3','HYPE3','BBDC4',
             'SANB3','TIET4','ITUB3','ALUP4','B3SA3','BIDI4','BMGB4','BPAN4',
             'SANB4','BBDC3','BPAR3','BGIP3','BGIP4','BRML3','BRFS3','BBSE3',
             'BPAC3','CEAB3','CRFB3','LCAM3','CSNA3','CPLE3','CCRO3','FJTA4'] # lista de acoes a serem calculadas

#funcao que faz a insercao dos dados no banco de dados. Ela cria uma tabela e uma sequence para cada acao,
#depois seleciona o fechamento D e D+1, e subtrai 1 para encontrarmos a variacao diaria. Essa variacao diaria entao e inserida
#numa tabela de correlacao. Essa tabela de correlacao possui uma coluna index e uma coluna para cada acao. Na coluna de cada acao
#sao inseridos as variacoes diarias.
def longShort(acao):
    print("Carregando "+acao)
    bdInput('create table '+acao+' ( id smallint,fechamento numeric,variacao numeric)')
    bdInput('create sequence '+acao+'_SEQ start with 1')
    with open(arquivo) as file:
        for line in file:
            if len(acao) == 5 and line[12:17] == acao and line[17] == ' ':
                bdInput("insert into "+acao+" (id,fechamento) values (nextval('"+acao+"_SEQ'),'"+str(float(line[109:121])/100)+"');")
            elif len(acao) == 6 and line[12:17] == acao[0:5] and acao[4] in filtro1:
                bdInput("insert into "+acao+" (id,fechamento) values (nextval('"+acao+"_SEQ'),'"+str(float(line[109:121])/100)+"');")
    last = int(bdSelect("SELECT max(id) from "+acao+";")[0][0])
    bdInput("alter table corr add column "+acao+" numeric;")
    for indice in range(1,last):
        valor1=float(bdSelect('select fechamento from '+acao+' where id ='+str(indice)+';')[0][0])
        valor2=float(bdSelect('select fechamento from '+acao+' where id >'+str(indice)+';')[0][0])
        vari=str((valor1/valor2)-1)
        bdInput('insert into corr (id,'+acao+') values ('+str(indice)+','+vari+') on conflict on constraint corr_pkey do update set '+acao+' = '+vari+';')
    print("Terminado "+acao)
dataDownload()
dataCleaner()
bdReset()

with ThreadPoolExecutor(max_workers=3) as executor:
    executor.map(longShort,listaAcao)

folder=os.listdir('.')
for file in folder:
    if file.endswith("txt"):
        os.remove(file)
spec='itsa3'

print("correlacao de 247 periodos: \n"+str(bdPandas(247))+'\n')
print("correlacao de 180 periodos: \n"+str(bdPandas(180))+'\n')
print("correlacao de 90 periodos: \n"+str(bdPandas(90))+'\n')
print("correlacao de 45 periodos: \n"+str(bdPandas(45))+'\n')
print("correlacao de 247 periodos da "+spec+": \n"+str(bdPandasSpec(spec,247))+'\n')
