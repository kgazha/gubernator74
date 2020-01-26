import pandas as pd
import pymorphy2

from collections import Counter
import models
import config
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from nltk import ngrams
import nltk
from nltk.tag import pos_tag
from nltk.corpus import stopwords
from pymystem3 import Mystem
from string import punctuation


morph = pymorphy2.MorphAnalyzer()
tokenizer = nltk.data.load('tokenizers/punkt/russian.pickle')
session = sessionmaker(bind=config.ENGINE)()
mystem = Mystem() 
russian_stopwords = stopwords.words("russian")


def get_normalized_words_from_text(text, use_main_pos=True):
    translator = str.maketrans({key: " " for key in punctuation})
    cleaned_text = " ".join(text.translate(translator).split())
    normalized_words = []
    for word in cleaned_text.split():
        if len(word) < 3:
            normalized_words.append(word.lower())
            continue
        normal_form = None
        for parse_word in morph.parse(word):
            if use_main_pos and parse_word.tag.POS in ['NUMR', 'NPRO', 'PREP', 'CONJ', 'PRCL', 'INTJ']:
                continue
            if parse_word.tag.number:
                if parse_word.tag.number == 'sing'\
                        and parse_word.score >= max(map(lambda x: x.score, morph.parse(word))):
                    if normal_form is None:
                        normal_form = parse_word.normal_form
                    elif parse_word.tag.POS == 'NOUN' and not word.islower():
                        normal_form = parse_word.normal_form
                        break
        if normal_form:
            normalized_words.append(normal_form)
        else:
            normalized_words.append(morph.parse(word)[0].normal_form.lower())
    return normalized_words

def get_cleaned_comments(comments):
    cleaned_comments = []
    for comment in comments:
        normalized_words = get_normalized_words_from_text(comment.text)
        without_stopwords = [word for word in normalized_words if word.lower() not in stopwords.words('russian')]
        cleaned_comments.append(without_stopwords)
    return cleaned_comments

def get_bigrams_from_words(words):
    bigrams = []
    for _bigram in ngrams(words, 2):
        bigrams.append(_bigram)
    return bigrams

def text_to_sentences(text):
    return tokenizer.tokenize(text)

def lemmatize_text(text):
    tokens = mystem.lemmatize(text.lower())
    tokens = [token for token in tokens if token not in russian_stopwords\
                and token != " " \
                and token.strip() not in punctuation]
    text = " ".join(tokens)
    return text

def sentence_to_lower_words(sentence):
    return [word.lower() for word in sentence.split(' ')]

def get_sentence_type(model, sentence):
    if sentence[-1] == '!':
        s_type = session.query(model).filter(model.name == 'Восклицательное').first()
    elif sentence[-1] == '?':
        s_type = session.query(model).filter(model.name == 'Вопросительное').first()
    else:
        s_type = session.query(model).filter(model.name == 'Повествовательное').first()
    return s_type


class Analysis:
    def __init__(self, comments):
        self.comments = comments
        self.cleaned_comments = None

    def comment_keywords_to_database(self):
        if not self.cleaned_comments:
            self.cleaned_comments = get_cleaned_comments(self.comments)
        for idx, comment in enumerate(self.cleaned_comments):
            keywords_frequencies = dict(Counter(comment))
            for key, value in keywords_frequencies.items():
                keyword = models.get_or_create(session, models.Keyword, name=key)[0]
                comment_keyword = models.get_or_create(session, models.CommentKeyword,
                                                       comment_id=self.comments[idx].id,
                                                       keyword_id=keyword.id)[0]
                comment_keyword.frequency = value
                self.comments[idx].keywords.append(comment_keyword)
            self.comments[idx].last_keyword_analysis = datetime.now()
            session.commit()

    def sentences_to_database(self):
        for idx, comment in enumerate(self.comments):
            sentences = text_to_sentences(comment.text)
            for s_order, sentence in enumerate(sentences):
                sentence_obj = models.get_or_create(session, models.Sentence,
                                                    comment_id=comment.id,
                                                    order=s_order)[0]
                sentence_obj.text = sentence
                sentence_type = get_sentence_type(models.SentenceType, sentence)
                sst = models.get_or_create(session, models.SST,
                                           sentence_id=sentence_obj.id,
                                           sentence_type_id=sentence_type.id)[0]
                sentence_words = get_normalized_words_from_text(sentence)
                for w_order, word in enumerate(sentence_words):
                    word_obj = models.get_or_create(session, models.Word, name=word)[0]
                    sentence_word = models.get_or_create(session, models.SentenceWord,
                                                         word_id=word_obj.id,
                                                         sentence_id=sentence_obj.id)[0]
                    sentence_word.order = w_order
                session.commit()

    def bigrams_to_database(self):
        if not self.cleaned_comments:
            self.cleaned_comments = get_cleaned_comments(self.comments)
        for idx, comment in enumerate(self.cleaned_comments):
            bigrams = get_bigrams_from_words(comment)
            bigram_frequencies = dict(Counter(bigrams))
            for key, value in bigram_frequencies.items():
                normalized_first_word = morph.parse(key[0])[0].normal_form
                normalized_second_word = morph.parse(key[1])[0].normal_form
                first_keyword_id = models.get_or_create(session, models.Keyword,
                                                        name=normalized_first_word)[0].id
                second_keyword_id = models.get_or_create(session, models.Keyword,
                                                         name=normalized_second_word)[0].id
                bigram = models.get_or_create(session, models.Bigram,
                                              first_keyword_id=first_keyword_id,
                                              second_keyword_id=second_keyword_id)[0]
                comment_bigram = models.get_or_create(session, models.CommentBigram,
                                                      comment_id=self.comments[idx].id,
                                                      bigram_id=bigram.id)[0]
                comment_bigram.frequency = value
            self.comments[idx].last_bigram_analysis = datetime.now()
            session.commit()


if __name__ == '__main__':
    _comments = session.query(models.Comment).filter(models.Comment.last_keyword_analysis.is_(None)).all()
    analyser = Analysis(_comments)
    # analyser.comments = session.query(models.Comment).all()
    analyser.comment_keywords_to_database()
    analyser.comments = session.query(models.Comment).filter(models.Comment.last_bigram_analysis.is_(None)).all()
    # analyser.comments = session.query(models.Comment).all()
    analyser.bigrams_to_database()
    # analyser.sentences_to_database()
