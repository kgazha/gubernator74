from datetime import datetime
# from instaparser.agents import Agent
# from instagram import WebAgent
# from instaparser.entities import Account, Media
from instaparser.exceptions import InternetException
import models
import config
from sqlalchemy.orm import sessionmaker
import time
import logging
from instagram import Account, Media, WebAgent


logging.basicConfig(filename='instagram_parser.log',
                    filemode='w',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
session = sessionmaker(bind=config.ENGINE)()


class InstagramParser:
    def __init__(self, instagram_login):
        self.agent = WebAgent()
        self.instagram_login = instagram_login
        self.account = Account(self.instagram_login)

    def __get_comments(self, post_media, comments=None, pointer=None, last_comment_id=None):
        _comments = []
        _pointer = None
        if comments:
            _comments = comments
        if pointer:
            _pointer = pointer
        try:
            _media, _pointer = self.agent.get_comments(post_media, pointer=_pointer, delay=5)
        # except InternetException as ex:
        except Exception as ex:
            print(ex)
            logging.warning(ex)
            return self.__get_comments(post_media, _comments, _pointer, last_comment_id)
        _comments += _media
        if last_comment_id:
            if last_comment_id in [int(comment.id) for comment in _media]:
                return _comments
        if _pointer:
            return self.__get_comments(post_media, _comments, _pointer, last_comment_id)
        return _comments

    def __download_comments_from_post(self, post_media):
        logging.info('Getting comments')
        last_comment = session.query(models.Comment).filter_by(post_id=post_media.id) \
            .order_by(models.Comment.date.desc()).first()
        if last_comment:
            comments = self.__get_comments(post_media, last_comment_id=last_comment.id)
        else:
            comments = self.__get_comments(post_media)
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
        for idx in range(len(media), 0, -1):
            logging.info('Start parsing post ' + media[-idx].code)
            post_media = Media(media[-idx])
            comments = self.__download_comments_from_post(post_media)
            post_owner = models.get_or_create(session, models.Account, name=self.account.username)[0]
            post = models.get_or_create(session, models.Post, id=post_media.id,
                                        code=post_media.code, owner_id=post_owner.id)[0]
            post.caption = post_media.caption
            post.date = datetime.fromtimestamp(post_media.date)
            for comment in comments:
                _author = models.get_or_create(session, models.Account, name=comment['Author'].username)[0]
                _comment = models.get_or_create(session, models.Comment,
                                                id=comment['Id'], post_id=post.id)[0]
                _comment.date = comment['Date']
                _comment.text = comment['Text']
                _comment.author_id = _author.id
            session.commit()


if __name__ == '__main__':
    instagram_parser = InstagramParser("alexeytexler.official")
    instagram_parser.save_comments_from_posts()
