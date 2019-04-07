from datetime import datetime
from instaparser.agents import Agent
from instaparser.entities import Account, Media
from instaparser.exceptions import InternetException
import models
import config
from sqlalchemy.orm import sessionmaker
import time
import logging


logging.basicConfig(filename='instagram_parser.log',
                    filemode='w',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
session = sessionmaker(bind=config.ENGINE)()


class InstagramParser:
    def __init__(self, instagram_login):
        self.agent = Agent()
        self.instagram_login = instagram_login
        self.account = Account(self.instagram_login)

    def __get_comments(self, post_media, comments=[], pointer=None):
        try:
            _media, _pointer = self.agent.get_comments(post_media, pointer=pointer, delay=5)
        except InternetException as ex:
            print(ex)
            logging.warning(ex)
            return self.__get_comments(post_media, comments, _pointer)
        comments += _media
        if _pointer:
            return self.__get_comments(post_media, comments, _pointer)
        return comments


    def __download_comments_from_post(self, post_media):
        logging.info('Getting comments')
        comments = self.__get_comments(post_media)
        print(len(comments))
        print(len(list(set(comments))))
        comments = list(set(comments))
        post_comments = []
        for comment in comments:
            comment_info = {'Id': comment.id,
                            'Date': datetime.fromtimestamp(comment.created_at),
                            'Text': comment.text,
                            'Author': comment.owner}
            post_comments.append(comment_info)
        return post_comments

    def save_comments_from_posts(self, count=20):
        media, pointer = self.agent.get_media(self.account, count=count, delay=5)
        for idx in range(len(media), 1, -1):
            logging.info('Start parsing post ' + media[-idx].code)
            post_media = Media(media[-idx])
            comments = self.__download_comments_from_post(post_media)
            post_owner = models.get_or_create(session, models.Account, name=self.account.login)[0]
            post = models.get_or_create(session, models.Post, id=post_media.id, code=post_media.code,
                                        caption=post_media.caption, owner_id=post_owner.id,
                                        date=datetime.fromtimestamp(post_media.date))[0]
            for comment in comments:
                _author = models.get_or_create(session, models.Account, name=comment['Author'].login)[0]
                _comment = models.get_or_create(session, models.Comment,
                                                id=comment['Id'], post_id=post.id)[0]
                _comment.date = comment['Date']
                _comment.text = comment['Text']
                _comment.author_id = _author.id
            # session.flush()
            session.commit()


if __name__ == '__main__':
    instagram_parser = InstagramParser("alexeytexler.official")
    instagram_parser.save_comments_from_posts()
