import urllib
import re

class Scraper:

    def __init__(self,url):
        self.url = url
        self.html = ""

    def get_html(self, url):
        fp = urllib.request.urlopen(url)
        mybytes = fp.read()
        html = mybytes.decode("utf8")
        fp.close()
        self.html = html
        return html

    def cleanhtml(self, raw_html):
        cleaner = re.compile('<.*?>')
        cleantext = re.sub(cleaner, '', raw_html)
        return cleantext
