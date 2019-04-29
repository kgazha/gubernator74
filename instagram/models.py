import config
from sqlalchemy.sql import ClauseElement
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, ForeignKey, Table, Float

Base = declarative_base()


class Account(Base):
    __tablename__ = 'account'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    # post = relationship("Post")
    # comment = relationship("Comment")


class CommentKeyword(Base):
    __tablename__ = 'comment_keyword'
    id = Column(Integer, primary_key=True)
    comment_id = Column(BigInteger, ForeignKey('comment.id'))
    keyword_id = Column(Integer, ForeignKey('keyword.id'))
    frequency = Column(String)
    # keyword = relationship("Keyword", cascade="save-update, merge, delete")


class SentenceWord(Base):
    __tablename__ = 'sentence_word'
    id = Column(Integer, primary_key=True)
    word_id = Column(Integer, ForeignKey('word.id'))
    sentence_id = Column(Integer, ForeignKey('sentence.id'))
    order = Column(Integer)


class Sentence(Base):
    __tablename__ = 'sentence'
    id = Column(Integer, primary_key=True)
    text = Column(String)
    comment_id = Column(BigInteger, ForeignKey('comment.id'))
    order = Column(Integer)


class SST(Base):
    __tablename__ = 'sst'
    sentence_id = Column(Integer, ForeignKey('sentence.id', ondelete='CASCADE'), primary_key=True)
    sentence_type_id = Column(Integer, ForeignKey('sentence_type.id', ondelete='CASCADE'), primary_key=True)


class SentenceType(Base):
    __tablename__ = 'sentence_type'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Bigram(Base):
    __tablename__ = 'bigram'
    id = Column(BigInteger, primary_key=True)
    first_keyword_id = Column(Integer, ForeignKey('keyword.id', ondelete='CASCADE'))
    second_keyword_id = Column(Integer, ForeignKey('keyword.id', ondelete='CASCADE'))


class ThemeBigram(Base):
    __tablename__ = 'theme_bigram'
    id = Column(BigInteger, primary_key=True)
    theme_id = Column(Integer, ForeignKey('theme.id', ondelete='CASCADE'))
    bigram_id = Column(Integer, ForeignKey('bigram.id', ondelete='CASCADE'))


class CommentBigram(Base):
    __tablename__ = 'comment_bigram'
    id = Column(Integer, primary_key=True)
    comment_id = Column(BigInteger, ForeignKey('comment.id'))
    bigram_id = Column(Integer, ForeignKey('bigram.id'))
    frequency = Column(String)


class Post(Base):
    __tablename__ = 'post'
    id = Column(BigInteger, primary_key=True)
    code = Column(String, unique=True)
    owner_id = Column(Integer, ForeignKey('account.id'))
    caption = Column(String)
    date = Column(DateTime)
    # comment = relationship("Comment")
    # keywords = relationship("PostKeyword")


class Comment(Base):
    __tablename__ = 'comment'
    id = Column(BigInteger, primary_key=True)
    post_id = Column(BigInteger, ForeignKey('post.id'))
    author_id = Column(Integer, ForeignKey('account.id'))
    text = Column(String)
    date = Column(DateTime)
    last_keyword_analysis = Column(DateTime)
    keywords = relationship("CommentKeyword", cascade="save-update, merge, delete")


class ThemeKeyword(Base):
    __tablename__ = 'theme_keyword'
    theme_id = Column(Integer, ForeignKey('theme.id', ondelete='CASCADE'), primary_key=True)
    keyword_id = Column(Integer, ForeignKey('keyword.id', ondelete='CASCADE'), primary_key=True)
    # keyword = relationship("Keyword")


class Word(Base):
    __tablename__ = 'word'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Keyword(Base):
    __tablename__ = 'keyword'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Theme(Base):
    __tablename__ = 'theme'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    theme_type_id = Column(Integer, ForeignKey('theme_type.id'), nullable=True)
    # keywords = relationship("ThemeKeyword")


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


def drop_tables():
    Base.metadata.drop_all(config.ENGINE)


def create_tables():
    Base.metadata.create_all(config.ENGINE)


if __name__ == '__main__':
    create_tables()
