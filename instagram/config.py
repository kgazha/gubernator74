from sqlalchemy import create_engine


DATABASE_NAME = 'instagram'
DATABASE_HOST = 'localhost'
DATABASE_USER = 'postgres'
DATABASE_PASSWORD = 'admin'
DATABASE_ENGINE = 'postgresql+psycopg2'
ENGINE = create_engine('{0}://{1}:{2}@{3}/{4}'.format(DATABASE_ENGINE,
                                                      DATABASE_USER,
                                                      DATABASE_PASSWORD,
                                                      DATABASE_HOST,
                                                      DATABASE_NAME))
