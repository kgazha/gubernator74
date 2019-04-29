import unittest
from instagram_analysis import AnalysisTools


class TestAnalysisTools(unittest.TestCase):

    def test_text_to_sentences(self):
        texts = ['Здравствуйте, Текслер! Как дела? Обратите внимание на состояние дорог',
                 'Ай да А.С. Пушкин! Ай да сукин сын!',
                 'ул. Харлова, д.20. Как вам это здание?']
        sentences = [AnalysisTools.text_to_sentences(text) for text in texts]
        goal = [['Здравствуйте, Текслер!', 'Как дела?', 'Обратите внимание на состояние дорог'],
                ['Ай да А.С. Пушкин!', 'Ай да сукин сын!'],
                ['ул. Харлова, д.20.', 'Как вам это здание?']]
        self.assertEqual(sentences, goal)

    def test_sentence_to_lower_words(self):
        sentence = 'ул. Харлова, д.20. Как вам это здание?'
        cleaned_text = AnalysisTools.sentence_to_lower_words(sentence)
        goal = [['ул.', 'Харлова', 'д.20.'], ['как', 'вам', 'это', 'здание?']]
        self.assertEqual(cleaned_text, goal)

    def test_get_normalized_words_from_text(self):
        sentence = 'ул. Харлова, д.20. Как вам это здание?'
        normalized_words = AnalysisTools.get_normalized_words_from_text(sentence, False)
        goal = ['ул', 'харлов', 'д', '20', 'как', 'вам', 'это', 'здание']
        self.assertEqual(normalized_words, goal)
