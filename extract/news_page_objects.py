from numpy.core.numeric import NaN
from common import config
import requests
import bs4
class NewsPage:
    """
    docstring
    """
    def __init__(self, news_site_uid, url):
        """
        docstring
        """
        self._config = config()['news_sites'][news_site_uid]
        self._queries = self._config['queries']
        self._html = None
        self._url = url
        self._visit(url)


    def _visit(self, url):
        """
        docstring
        """
        response = requests.get(url)
        response.raise_for_status()
        self._html = bs4.BeautifulSoup(response.text, 'html.parser')
    
    def _select(self, query_string):
        """
        docstring
        """
        return self._html.select(query_string)

class HomePage(NewsPage):
    """
    docstring
    """
    def __init__(self, news_site_uid, url):
        """
        docstring
        """
        super().__init__(news_site_uid, url)


    @property
    def article_links(self):
        """
        docstring
        """
        link_list = []
        for link in self._select(self._queries['homepage_article_links']):
            if link and link.has_attr('href'):
                link_list.append(link)
        return set(link['href'] for link in link_list)


class ArticlePage(NewsPage):
    """
    docstring
    """
    def __init__(self, news_site_uid, url):
        """
        docstring
        """
        super().__init__(news_site_uid, url)

    @property
    def title(self):
        """
        docstring
        """
        result = self._select(self._queries['article_title'])
        return result[0].text if len(result) else NaN

    @property
    def body(self):
        """
        docstring
        """
        result = self._select(self._queries['article_body'])
        return result[0].text if len(result) else NaN

    @property
    def url(self):
        """
        docstring
        """
        result = self._url
        return result if len(result) else NaN