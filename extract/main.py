import argparse
import logging
import csv
import datetime
logging.basicConfig(level=logging.INFO)
import news_page_objects as news
from common import config
import re

from requests.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError

logger = logging.getLogger(__name__)
# La 'r' antes del string indica a python que es un string raw
# ^inicio de la palabra, ?opcional, .+ que tenga uno o mas letras
# $terminamos nuestro patron.
is_well_formed_link = re.compile(r'^https?://.+/.+$') # https://example.com/hello
is_root_path = re.compile(r'^/.+$') # /sometext/somemore

def _news_scraper(news_site_uid):
	"""
	docstring
	"""
	host = config()['news_sites'][news_site_uid]['url']
	logging.info(f'Beginning scraper for {host}')
	homepage = news.HomePage(news_site_uid, host)
	articles = []
	for link in homepage.article_links:
		article = _fetch_article(news_site_uid, host, link)
		if article:
			logger.info('Article fetched!! That\'s right!')
			articles.append(article)
			# Just for one
			# break

	_save_articles(news_site_uid, articles)

def _save_articles(news_site_uid, articles):
	"""
	docstring
	"""
	now = datetime.datetime.now().strftime('%Y_%m_%d')
	out_filename = f'{news_site_uid}_{now}_articles.csv'
	csv_headers = list(filter(lambda property: not property.startswith('_'), dir(articles[0])))
	with open(out_filename, mode='w+', encoding = 'utf-8') as f:
		writer = csv.writer(f)
		writer.writerow(csv_headers)
		for article in articles:
			row = [str(getattr(article, prop)) for prop in csv_headers]
			writer.writerow(row)

def _fetch_article(news_site_uid, host, link):
	"""
	docstring
	"""
	logger.info(f'Start fetching article pages at {link}')
	article = None
	try:
		article = news.ArticlePage(news_site_uid, _build_link(host, link))
	except (HTTPError, MaxRetryError) as e:
		logger.warning('Error while fetching the article', exc_info=False)
	if article and not article.body:
		logger.warning('Invalid or empty article body')
		return None
	return article


def _build_link(host, link):
	"""
	docstring
	"""
	if is_well_formed_link.match(link):
		return link
	elif is_root_path.match(link):
		return(f'{host}{link}')
	else:
		return(f'{host}/{link}')

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	news_site_choices = list(config()['news_sites'].keys())
	parser.add_argument('news_sites',
						help='The news website that you want to scrape',
						type=str,
						choices=news_site_choices)
	args = parser.parse_args()
	_news_scraper(args.news_sites)