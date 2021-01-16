import argparse
import hashlib
import logging
import nltk
from nltk.corpus import stopwords
stop_words = set(stopwords.words('spanish'))

nltk.download('punkt')
nltk.download('stopwords')
logging.basicConfig(level=logging.INFO)

from urllib.parse import urlparse
import pandas as pd

logger = logging.getLogger(__name__)

def main(filename):
    """
    docstring
    """
    logger.info('Starting cleaning process')
    df = _read_data(filename)
    newspaper_uid = _extract_news_paper_uid(filename)
    df = _add_newspaper_uid_column(df, newspaper_uid)
    df = _extract_host(df)
    df = _fill_missing_titles(df)
    df = _generate_uids_for_rows(df)
    df = _format_body(df)
    df = _n_tokenize(df, 'title')
    df = _n_tokenize(df, 'body')
    df = _remove_duplicates_enties(df, 'title')
    df = _drop_rows_with_missing_values(df)
    _save_data(df, filename)

    return df

def _read_data(filename):
    """
    docstring
    """
    logger.info(f'Reading file {filename}...')
    return pd.read_csv(filename)

def _extract_news_paper_uid(filename):
    """
    docstring
    """
    logger.info(f'Extracting newspaper_uid...')
    news_paper_uid = filename.split('_')[0]
    logger.info(f'Newspaper UID detected: {news_paper_uid}')
    return news_paper_uid

def _add_newspaper_uid_column(df, newspaper_uid):
    """
    docstring
    """
    logger.info(f'Filling newspaper_uid colum with {newspaper_uid}')
    df['newspaper_uid'] = newspaper_uid
    return df

def _extract_host(df):
    """
    docstring
    """
    logger.info(f'Extracting Host from urls...')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    return df

def _fill_missing_titles(df):
    logger.info('Filling missing titles')
    missing_titles_mask = df['title'].isna()
    missing_titles = (df[missing_titles_mask]['url']
                        .str.extract(r'(?P<missing_titles>[^/]+)$')
                        .applymap(lambda title: title.split('-'))
                        .applymap(lambda title_word_list: ' '.join(title_word_list))
                     )

    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']
    return df

def _generate_uids_for_rows(df):
    """
    docstring
    """
    logger.info('Generating Uids for each row....')
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())) , axis=1)
            .apply(lambda hash_object: hash_object.hexdigest())
            )
    df["UIDS"] = uids
    df.set_index('uid', inplace=True)
    return(df)


def _format_body(df):
    """
    docstring
    """
    logger.info('Fomatting body....')
    body = (df
            .apply(lambda row: row['body'], axis=1)
            .apply(lambda body: body.replace('\n',''))
            .apply(lambda body: body.replace('\r',''))
            )
    df['body'] = body
    return df

def _n_tokenize(df, column_name):
    logger.info('Tokenizing words....')
    valid_words = (df
            .dropna()
            .apply(lambda row: nltk.word_tokenize(row[column_name]), axis = 1)
            .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
            .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
            .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
            .apply(lambda valid_word_list: len(valid_word_list))
            )
    df[f'n_tokens_{column_name}'] = valid_words
    return df

def _remove_duplicates_enties(df, column_name):
    """
    docstring
    """
    logger.info('Removing duplicate values...')
    df.drop_duplicates(subset=[column_name], keep='first', inplace=True)
    return df

def _drop_rows_with_missing_values(df):
    """
    docstring
    """
    logger.info('Droping rows with missing data...')
    df.dropna()
    return df

def _save_data(df, filename):
    """
    docstring
    """
    clean_filename = f'clean{filename}.csv'
    logger.info(f'Saving data in {clean_filename}')
    df.to_csv(clean_filename)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The path to the dirty data',
                        type=str)
    arg = parser.parse_args()
    df = main(arg.filename)
    print(df)