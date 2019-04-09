import config
from sqlalchemy.sql import ClauseElement
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, ForeignKey, Table, Float

Base = declarative_base()


class Account(Base):
    __tablename__ = 'account'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    post = relationship("Post")
    comment = relationship("Comment")


class PostKeyword(Base):
    __tablename__ = 'post_keyword'
    post_id = Column(BigInteger, ForeignKey('post.id'), primary_key=True)
    keyword_id = Column(Integer, ForeignKey('keyword.id'), primary_key=True)
    frequency = Column(String)
    keyword = relationship("Keyword")


class CommentKeyword(Base):
    __tablename__ = 'comment_keyword'
    comment_id = Column(BigInteger, ForeignKey('comment.id'), primary_key=True)
    keyword_id = Column(Integer, ForeignKey('keyword.id'), primary_key=True)
    frequency = Column(String)
    keyword = relationship("Keyword")


class Post(Base):
    __tablename__ = 'post'
    id = Column(BigInteger, primary_key=True)
    code = Column(String, unique=True)
    owner_id = Column(Integer, ForeignKey('account.id'))
    caption = Column(String)
    date = Column(DateTime)
    comment = relationship("Comment")
    keywords = relationship("PostKeyword")


class Comment(Base):
    __tablename__ = 'comment'
    id = Column(BigInteger, primary_key=True)
    post_id = Column(BigInteger, ForeignKey('post.id'))
    author_id = Column(Integer, ForeignKey('account.id'))
    text = Column(String)
    date = Column(DateTime)
    last_keyword_analysis = Column(DateTime)
    keywords = relationship("CommentKeyword")


class ThemeKeyword(Base):
    __tablename__ = 'theme_keyword'
    theme_id = Column(Integer, ForeignKey('theme.id'), primary_key=True)
    keyword_id = Column(Integer, ForeignKey('keyword.id'), primary_key=True)
    weight = Column(Float)
    keyword = relationship("Keyword")


class Keyword(Base):
    __tablename__ = 'keyword'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Theme(Base):
    __tablename__ = 'theme'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    theme_type_id = Column(Integer, ForeignKey('theme_type.id'), nullable=True)
    keywords = relationship("ThemeKeyword")


class ThemeType(Base):
    __tablename__ = 'theme_type'
    id = Column(Integer, primary_key=True)
    name = Column(String)


def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        params = dict((k, v) for k, v in kwargs.items() if not isinstance(v, ClauseElement))
        instance = model(**params)
        session.add(instance)
        session.flush()
        return instance, True


Base.metadata.create_all(config.ENGINE)
