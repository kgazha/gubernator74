import pandas as pd
import pymorphy2
from nltk.corpus import stopwords
from collections import Counter
import string
import keywords
import models
import config
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from nltk import ngrams


morph = pymorphy2.MorphAnalyzer()
session = sessionmaker(bind=config.ENGINE)()


class Analysis:
    def __init__(self):
        self.normalized_comments = []
        self.bigrams = []
        self.comments = [q for q in session.query(models.Comment)]

    @staticmethod
    def get_normalized_words_from_text(text):
        translator = str.maketrans({key: " " for key in string.punctuation})
        cleaned_text = " ".join(text.translate(translator).split())
        normalized_words = []
        for word in cleaned_text.split():
            for parse_word in morph.parse(word):
                if parse_word.tag.POS in ['NUMR', 'NPRO', 'PREP', 'CONJ', 'PRCL', 'INTJ']:
                    break
                if parse_word.tag.number:
                    if parse_word.tag.number == 'sing':
                        normalized_words.append(parse_word.normal_form)
                        break
        return normalized_words

    @staticmethod
    def filter_stop_words(words):
        return [word for word in words if word.lower() not in stopwords.words('russian')]

    def get_normalized_comment_words(self, comments=None):
        for comment in comments:
            normalized_words = Analysis.get_normalized_words_from_text(comment.text)
            self.normalized_comments.append(Analysis.filter_stop_words(normalized_words))

    def normalized_comment_words_to_database(self):
        if not self.normalized_comments:
            self.get_normalized_comment_words(self.comments)
        for idx, comment in enumerate(self.normalized_comments):
            keywords_frequencies = dict(Counter(comment))
            session.query(models.CommentKeyword).filter_by(comment_id=self.comments[idx].id).delete()
            for key, value in keywords_frequencies.items():
                keyword = models.get_or_create(session, models.Keyword, name=key)[0]
                comment_keyword = models.get_or_create(session, models.CommentKeyword,
                                                       comment_id=self.comments[idx].id,
                                                       keyword_id=keyword.id)[0]
                comment_keyword.frequency = value
                comment_keyword.keyword = keyword
                self.comments[idx].keywords.append(comment_keyword)
                session.flush()
                session.commit()
            session.commit()

    def comment_keywords_to_database(self):
        comments = session.query(models.Comment).filter(models.Comment.last_keyword_analysis.is_(None)).all()
        if not self.normalized_comments:
            self.get_normalized_comment_words(comments)
        for idx, comment in enumerate(self.normalized_comments):
            keywords_frequencies = dict(Counter(comment))
            # session.query(models.CommentKeyword).filter_by(comment_id=self.comments[idx].id).delete()
            for key, value in keywords_frequencies.items():
                keyword = models.get_or_create(session, models.Keyword, name=key)[0]
                comment_keyword = models.get_or_create(session, models.CommentKeyword,
                                                       comment_id=self.comments[idx].id,
                                                       keyword_id=keyword.id)[0]
                comment_keyword.frequency = value
                comment_keyword.keyword = keyword
                comments[idx].keywords.append(comment_keyword)
                # session.flush()
                # session.commit()
            comments[idx].last_keyword_analysis = datetime.now()
            session.commit()

    @staticmethod
    def manual_themes_to_database():
        for key in keywords.theme_keywords:
            theme = models.get_or_create(session, models.Theme, name=key)[0]
            for keyword in keywords.theme_keywords[key]:
                _keyword = models.get_or_create(session, models.Keyword, name=keyword)[0]
                theme_keyword = models.get_or_create(session, models.ThemeKeyword,
                                                     theme_id=theme.id, keyword_id=_keyword.id)[0]
                theme_keyword.keyword = _keyword
                theme.keywords.append(theme_keyword)
                session.flush()
        session.commit()

    @staticmethod
    def comment_themes_matrix_to_excel():
        query = """
        SELECT comment_id, frequency, keyword.name as keyword, theme.name as theme, comment.text 
        FROM public.comment_keyword
        INNER JOIN keyword on keyword_id = keyword.id
        INNER JOIN theme_keyword on theme_keyword.keyword_id = comment_keyword.keyword_id
        INNER JOIN theme on theme_id = theme.id
        INNER JOIN comment on comment_id = comment.id
        """
        df = pd.DataFrame(session.execute(query).fetchall(),
                          columns=['comment_id', 'frequency', 'keyword', 'theme', 'text'])
        comment_ids = list(set(df['comment_id']))
        comment_keywords = {}
        for idx, _id in enumerate(comment_ids):
            _keywords = {}
            _text = df[df['comment_id'] == _id]['text'].iloc[0]
            for _index, _data in df[df['comment_id'] == _id].iterrows():
                _keywords.update({_data['theme']: _data['frequency']})
            comment_keywords.update({_text: _keywords})
        df = pd.DataFrame(comment_keywords).T
        df.to_excel('comments.xlsx')

    def get_bigrams_from_words(self):
        if not self.normalized_comments:
            self.get_normalized_comment_words()
        for idx, comment in enumerate(self.normalized_comments):
            _bigrams = []
            for _bigram in ngrams(comment, 2):
                self.bigrams.append(_bigram)


if __name__ == '__main__':
    analyser = Analysis()
    # analyser.get_normalized_comment_words()
    # analyser.normalized_comment_words_to_database()
    analyser.comment_keywords_to_database()
    # Analysis.manual_themes_to_database()
