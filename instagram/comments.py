from datetime import datetime
import pandas as pd
from instaparser.agents import Agent, AgentAccount
from instaparser.entities import Account, Media, Comment
import pymorphy2
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from collections import defaultdict, Counter
import re
import string
import keywords
import models
import config
from sqlalchemy.orm import sessionmaker
import time


morph = pymorphy2.MorphAnalyzer()

session = sessionmaker(bind=config.ENGINE)()


class InstagramParser:
    def __init__(self, instagram_login):
        self.agent = Agent()
        self.instagram_login = instagram_login
        self.account = Account(self.instagram_login)

    def __get_comments(self, post_media, _delay=0, _limit=50):
        try:
            return list(self.agent.get_comments(post_media, count=post_media.comments_count,
                                                delay=_delay, limit=_limit))
        except Exception as ex:
            print(ex)
            _delay += 1
            if _limit > 20:
                _limit -= 10
            self.__get_comments(post_media, _delay, _limit)

    def __download_comments_from_post(self, post_media):
        comments = self.__get_comments(post_media)
        print(len(comments[0]))
        post_comments = []
        for comment in comments[0]:
            comment_info = {'Id': comment.id,
                            'Date': datetime.fromtimestamp(comment.created_at),
                            'Text': comment.text,
                            'Author': comment.owner}
            post_comments.append(comment_info)
        return post_comments

    def save_comments_from_posts(self, count=20):
        media, pointer = self.agent.get_media(self.account, count=count, delay=5)
        for idx in range(len(media), 0, -1):
            print('parsing post', media[-idx])
            post_media = Media(media[-idx])
            comments = self.__download_comments_from_post(post_media)
            post_owner = models.get_or_create(session, models.Account, name=self.account.login)[0]
            post = models.get_or_create(session, models.Post, id=post_media.id, code=post_media.code,
                                        caption=post_media.caption, owner_id=post_owner.id,
                                        date=datetime.fromtimestamp(post_media.date))[0]
            for comment in comments:
                _author = models.get_or_create(session, models.Account, name=comment['Author'].login)[0]
                _comment = models.get_or_create(session, models.Comment,
                                                id=comment['Id'], date=comment['Date'], post_id=post.id)[0]
                _comment.text = comment['Text']
                _comment.author_id = _author.id
        session.flush()
        session.commit()


class Analysis:
    def __init__(self):
        self.normalized_comments = []
        self.comments = [q for q in session.query(models.Comment)][:10]

    @staticmethod
    def get_normalized_words_from_text(text):
        translator = str.maketrans({key: " " for key in string.punctuation})
        cleaned_text = " ".join(text.translate(translator).split())
        return [morph.parse(word)[0].normal_form for word in cleaned_text.split()]

    @staticmethod
    def filter_stop_words(words):
        return [word for word in words if word.lower() not in stopwords.words('russian')]

    def get_normalized_comment_words(self):
        for comment in self.comments:
            normalized_words = Analysis.get_normalized_words_from_text(comment.text)
            self.normalized_comments.append(Analysis.filter_stop_words(normalized_words))

    def normalized_comment_words_to_database(self):
        if not self.normalized_comments:
            self.get_normalized_comment_words()
        for idx, comment in enumerate(self.normalized_comments):
            keywords_frequencies = dict(Counter(comment))
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
    def comment_themes_matrix():
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


instagram_parser = InstagramParser("alexeytexler.official")
instagram_parser.save_comments_from_posts()
print('start analyzing')
time_start = time.time()
analyser = Analysis()
analyser.normalized_comment_words_to_database()
Analysis.manual_themes_to_database()
Analysis.comment_themes_matrix()
print('end analyzing', time.time() - time_start)
