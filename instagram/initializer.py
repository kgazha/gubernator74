import models
import pymorphy2
import themes
import config
from instagram_parser import InstagramParser
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os


session = sessionmaker(bind=config.ENGINE)()
morph = pymorphy2.MorphAnalyzer()
test_folder = 'test_data'
file_models = {
    'account.csv': models.Account,
    'post.csv': models.Post,
    'comment.csv': models.Comment,
}
sentence_types = [
    'Повествовательное',
    'Вопросительное',
    'Восклицательное',
    'Побудительное',
    'Распространенное',
]


def themes_to_database():
    for theme_type_name, theme_type_data in themes.theme_keywords.items():
        theme_type = models.get_or_create(session, models.ThemeType, name=theme_type_name)[0]
        for theme_name, theme_keywords in theme_type_data.items():
            theme = models.get_or_create(session, models.Theme, name=theme_name)[0]
            theme.theme_type_id = theme_type.id
            for keyword in theme_keywords:
                normalized_keyword = morph.parse(keyword)[0].normal_form
                _keyword = models.get_or_create(session, models.Keyword, name=normalized_keyword)[0]
                theme_keyword = models.get_or_create(session, models.ThemeKeyword,
                                                     theme_id=theme.id, keyword_id=_keyword.id)[0]
        session.commit()

    for theme_type_name, theme_type_data in themes.theme_bigrams.items():
        theme_type = models.get_or_create(session, models.ThemeType, name=theme_type_name)[0]
        for theme_name, theme_bigrams in theme_type_data.items():
            theme = models.get_or_create(session, models.Theme, name=theme_name)[0]
            theme.theme_type_id = theme_type.id
            for bigram in theme_bigrams:
                normalized_first_word = morph.parse(bigram[0])[0].normal_form
                normalized_second_word = morph.parse(bigram[1])[0].normal_form
                first_keyword_id = models.get_or_create(session, models.Keyword,
                                                        name=normalized_first_word)[0].id
                second_keyword_id = models.get_or_create(session, models.Keyword,
                                                         name=normalized_second_word)[0].id
                _bigram = models.get_or_create(session, models.Bigram,
                                               first_keyword_id=first_keyword_id,
                                               second_keyword_id=second_keyword_id)[0]
                theme_bigram = models.get_or_create(session, models.ThemeBigram,
                                                    theme_id=theme.id, bigram_id=_bigram.id)[0]
        session.commit()


def sentence_types_to_database():
    for name in sentence_types:
        sentence_type = models.SentenceType(name=name)
        session.add(sentence_type)
        session.flush()
    session.commit()


def initial_data_to_database():
    for file_name, model in file_models.items():
        df = pd.read_csv(os.path.join(test_folder, file_name))
        for idx, row in df.iterrows():
            obj = model()
            for key, value in row.items():
                if value != 'None':
                    setattr(obj, key, value)
            session.add(obj)
            session.flush()
        session.commit()


def create_dump_files():
    def row_to_dict(row):
        d = {}
        for column in row.__table__.columns:
            d[column.name] = str(getattr(row, column.name))
        return d

    instagram_parser = InstagramParser("alexeytexler.official")
    instagram_parser.save_comments_from_posts(2)

    data = session.query(models.Comment).all()
    comments = [(row_to_dict(d)) for d in data]
    df = pd.DataFrame(comments)
    df.to_csv(os.path.join(test_folder, 'comment.csv'), index=False)

    data = session.query(models.Account).all()
    accounts = [(row_to_dict(d)) for d in data]
    df = pd.DataFrame(accounts)
    df.to_csv(os.path.join(test_folder, 'account.csv'), index=False)

    data = session.query(models.Post).all()
    posts = [(row_to_dict(d)) for d in data]
    df = pd.DataFrame(posts)
    df.to_csv('post.csv', index=False)


# models.drop_tables()
# models.create_tables()
themes_to_database()
sentence_types_to_database()
# initial_data_to_database()
