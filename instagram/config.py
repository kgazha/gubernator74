from sqlalchemy import create_engine


DATABASE_NAME = ''
DATABASE_HOST = ''
DATABASE_PASSWORD = ''
# DATABASE_NAME = 'instagram'
# DATABASE_HOST = 'localhost'
# DATABASE_PASSWORD = 'admin'
DATABASE_USER = 'postgres'
DATABASE_ENGINE = 'postgresql+psycopg2'
ENGINE = create_engine('{0}://{1}:{2}@{3}/{4}'.format(DATABASE_ENGINE,
                                                      DATABASE_USER,
                                                      DATABASE_PASSWORD,
                                                      DATABASE_HOST,
                                                      DATABASE_NAME))
