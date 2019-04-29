import pandas as pd
import pymorphy2
from nltk.corpus import stopwords
from collections import Counter
import string
import models
import config
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from nltk import ngrams
import nltk


morph = pymorphy2.MorphAnalyzer()
tokenizer = nltk.data.load('tokenizers/punkt/russian.pickle')
session = sessionmaker(bind=config.ENGINE)()


class AnalysisTools:
    @staticmethod
    def get_normalized_words_from_text(text, use_main_pos=True):
        translator = str.maketrans({key: " " for key in string.punctuation})
        cleaned_text = " ".join(text.translate(translator).split())
        normalized_words = []
        for word in cleaned_text.split():
            if len(word) < 3:
                normalized_words.append(word.lower())
                continue
            normal_form_found = False
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
                        # normalized_words.append(parse_word.normal_form)
                        normal_form_found = True
                        # continue
            print(normal_form)
            if normal_form:
                normalized_words.append(normal_form)
            # if not normal_form_found:
            else:
                normalized_words.append(morph.parse(word)[0].normal_form.lower())
        return normalized_words

    @staticmethod
    def get_cleaned_comments(comments):
        cleaned_comments = []
        for comment in comments:
            normalized_words = AnalysisTools.get_normalized_words_from_text(comment.text)
            without_stopwords = [word for word in normalized_words if word.lower() not in stopwords.words('russian')]
            cleaned_comments.append(without_stopwords)
        return cleaned_comments

    @staticmethod
    def get_bigrams_from_words(words):
        bigrams = []
        for _bigram in ngrams(words, 2):
            bigrams.append(_bigram)
        return bigrams

    @staticmethod
    def text_to_sentences(text):
        return tokenizer.tokenize(text)

    @staticmethod
    def sentence_to_lower_words(sentence):
        return [word.lower() for word in sentence.split(' ')]

    @staticmethod
    def get_sentence_type(model, sentence):
        if sentence[-1] == '!':
            s_type = session.query(model).filter(model.name == 'Восклицательное').first()
        elif sentence[-1] == '?':
            s_type = session.query(model).filter(model.name == 'Вопросительное').first()
        else:
            s_type = session.query(model).filter(model.name == 'Повествовательное').first()
        return s_type
    # @staticmethod
    # def get_normalized_word(word):
    #     for parse_word in morph.parse(word):
    #         if parse_word.tag.POS in ['NUMR', 'NPRO', 'PREP', 'CONJ', 'PRCL', 'INTJ']:
    #             break
    #         if parse_word.tag.number:
    #             if parse_word.tag.number == 'sing':
    #                 return parse_word.normal_form


class Analysis:
    def __init__(self, comments):
        self.comments = comments
        self.cleaned_comments = None

    def comment_keywords_to_database(self):
        if not self.cleaned_comments:
            self.cleaned_comments = AnalysisTools.get_cleaned_comments(self.comments)
        for idx, comment in enumerate(self.cleaned_comments):
            keywords_frequencies = dict(Counter(comment))
            for key, value in keywords_frequencies.items():
                keyword = models.get_or_create(session, models.Keyword, name=key)[0]
                # session.query(models.CommentKeyword).filter_by(comment_id=self.comments[idx].id).delete()
                comment_keyword = models.get_or_create(session, models.CommentKeyword,
                                                       comment_id=self.comments[idx].id,
                                                       keyword_id=keyword.id)[0]
                comment_keyword.frequency = value
                # comment_keyword.keyword = keyword
                self.comments[idx].keywords.append(comment_keyword)
            self.comments[idx].last_keyword_analysis = datetime.now()
            session.commit()

    def sentences_to_database(self):
        for idx, comment in enumerate(self.comments):
            sentences = AnalysisTools.text_to_sentences(comment.text)
            for s_order, sentence in enumerate(sentences):
                sentence_obj = models.get_or_create(session, models.Sentence,
                                                    comment_id=comment.id,
                                                    order=s_order)[0]
                sentence_obj.text = sentence
                sentence_type = AnalysisTools.get_sentence_type(models.SentenceType, sentence)
                sst = models.get_or_create(session, models.SST,
                                           sentence_id=sentence_obj.id,
                                           sentence_type_id=sentence_type.id)[0]
                sentence_words = AnalysisTools.get_normalized_words_from_text(sentence)
                for w_order, word in enumerate(sentence_words):
                    word_obj = models.get_or_create(session, models.Word, name=word)[0]
                    sentence_word = models.get_or_create(session, models.SentenceWord,
                                                         word_id=word_obj.id,
                                                         sentence_id=sentence_obj.id)[0]
                    sentence_word.order = w_order
                session.commit()

    def bigrams_to_database(self):
        if not self.cleaned_comments:
            self.cleaned_comments = AnalysisTools.get_cleaned_comments(self.comments)
        for idx, comment in enumerate(self.cleaned_comments):
            bigrams = AnalysisTools.get_bigrams_from_words(comment)
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
                # self.comments[idx].keywords.append(comment_keyword)
            # self.comments[idx].last_keyword_analysis = datetime.now()
            session.commit()


if __name__ == '__main__':
    _comments = session.query(models.Comment).filter(models.Comment.last_keyword_analysis.is_(None)).all()
    analyser = Analysis(_comments)
    # analyser.comment_keywords_to_database()
    analyser.comments = session.query(models.Comment).all()
    # analyser.sentences_to_database()
    print('>> bigrams')
    analyser.bigrams_to_database()

    # analyser.comments = session.query(models.Comment).all()
    # analyser.bigrams_to_database()
    # print(AnalysisTools.get_bigrams_from_words(['ставь', 'стакан', 'окно', 'закрыть']))
