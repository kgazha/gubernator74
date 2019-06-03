import pandas as pd
import os
import config
import models
from sqlalchemy.orm import sessionmaker


session = sessionmaker(bind=config.ENGINE)()
manual_themes = 'manual_themes'
cleaned_themes = 'cleaned_themes'
manual_territories = 'territories'
cleaned_territories = 'cleaned_territories'
comment_col = 'comment'
territory_col = 'territories'
territory_theme_type = 'МР/ГО'


def split_names(text):
    if pd.isnull(text):
        return []
    names = text.replace(';', ',').replace(':', ',').split(',')
    return names


def get_df_with_cleaned_names(df, from_col, to_col):
    names = []
    for idx, row in df.iterrows():
        names.append(split_names(row[from_col]))
    df[to_col] = names
    return df


def manual_themes_to_database(df):
    for idx, row in df.iterrows():
        comment = session.query(models.Comment).filter(models.Comment.text == row[comment_col]).first()
        if comment and (row[cleaned_themes] or row[cleaned_themes]):
            for theme_name in row[cleaned_themes]:
                theme = models.get_or_create(session, models.Theme, name=theme_name.strip())[0]
                theme_comment = models.get_or_create(session, models.ThemeComment,
                                                     theme_id=theme.id, comment_id=comment.id)[0]
            for territory in row[cleaned_territories]:
                theme_type = models.get_or_create(session, models.ThemeType, name=territory_theme_type)[0]
                theme = models.get_or_create(session, models.Theme, name=theme_name.strip(),
                                             theme_type_id=theme_type.id)[0]
                theme_comment = models.get_or_create(session, models.ThemeComment,
                                                     theme_id=theme.id, comment_id=comment.id)[0]
        session.commit()


def get_dataframes_from_directory(directory='./'):
    dataframes = []
    for subdir, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.xlsx'):
                df = pd.read_excel(file)
                df_cleaned = get_df_with_cleaned_names(df, manual_themes, cleaned_themes)
                df_cleaned = get_df_with_cleaned_names(df_cleaned, manual_territories, cleaned_territories)
                dataframes.append(df_cleaned)
    return dataframes


def get_dataframe_from_database():
    sql = '''SELECT comment_id, theme_id, theme.name
             FROM public.theme_comment
             LEFT JOIN theme ON theme.id = theme_id'''
    data = session.execute(sql).fetchall()
    df = pd.DataFrame(data)
    return df


if __name__ == '__main__':
    dataframes = get_dataframes_from_directory()
    df = pd.concat([df[[cleaned_themes, cleaned_territories, comment_col]] for df in dataframes])
    manual_themes_to_database(df)
