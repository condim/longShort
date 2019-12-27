import math, lxml.html as parser, requests, csv
from urllib.parse import urlsplit, urljoin

start_url = "https://www.fundamentus.com.br/detalhes.php?papel=PETR3"
r = requests.get(start_url)
html = parser.fromstring(r.text)

name = html.xpath("//td[@class='data w2']/span/text()")
valor = html.xpath("//td[@class='label w2']/span/text()")
for i in name:
    print(i+valor[list.index(i)])
