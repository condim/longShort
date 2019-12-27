import lxml.html as parser, requests, re
from concurrent.futures import ThreadPoolExecutor

listaAcao = ['PETR4','PETR3','VALE3','BBAS3','ITUB4','ABEV3','AZUL4','ANIM3',
             'BTOW3','BIDI3','BBDC3','BRPR3','BRKM3','BRSR3','ELET3','CESP3',
             'ELPL3','ENGI3','ECOR3','FJTA3','GFSA3','GGBR3','GOLL4','ITSA3',
             'LIGT3','LAME3','RENT3','GOAU3','MRVE3','ODPV3','OIBR3','RADL3',
             'ALPA3','SUZB3','TELB3','TOTS3','USIM3','UNIP3','UGPA3','VVAR3',
             'TIET3','ALUP3','CIEL3','SBSP3','EMBR3','GRND3','HYPE3','BBDC4',
             'SANB3','TIET4','ITUB3','ALUP4','B3SA3','BIDI4','BMGB4','BPAN4',
             'SANB4','BBDC3','BPAR3','BGIP3','BGIP4','BRML3','BRFS3','BBSE3',
             'BPAC3','CEAB3','CRFB3','LCAM3','CSNA3','CPLE3','CCRO3','FJTA4']

def fundamentos(acao):
    start_url = "https://www.fundamentus.com.br/detalhes.php?papel="+acao
    r = requests.get(start_url)
    html = parser.fromstring(r.text)

    name = html.xpath("//table[3]/*/td[@class='label w2']/span/text()")
    valor = html.xpath("//table[3]/*/td[@class='data w2']/span/text()")
    print("- "*10+acao+"- "*10)
    try:
        for i in range(0,50):
            f=(i*2)+1
            print(name[f]+" --- "+re.sub(r'\n','',re.sub(r'\ +','',valor[i])))
    except:
        pass

with ThreadPoolExecutor(max_workers=3) as executor:
    executor.map(fundamentos,listaAcao)