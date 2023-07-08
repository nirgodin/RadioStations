GENDER_PROMPT_FORMAT = "Given the name of a music artist, determine his or her gender of the following three options: "\
                       "'Male', 'Female' or 'Band'. In case you are not able to confidently determine the answer, " \
                       "return 'Unknown'. For example, given the following prompt 'The gender of John Lennon is', " \
                       "return 'Male'; given the following prompt 'The gender of Beyonce is', return 'Female'; Given " \
                       "The following prompt 'The gender of Pink Floyd is', return 'Band'. Your answer should include "\
                       "one token only. The gender of {} is"
OPENAI_MODEL = "text-davinci-003"
ARTIST_GENDER = 'artist_gender'
OPENAI_BASE_URL = 'https://api.openai.com/v1'
OPENAI_EMBEDDINGS_URL = f'{OPENAI_BASE_URL}/embeddings'
ADA_EMBEDDINGS_MODEL = 'text-embedding-ada-002'
EMBEDDING = 'embedding'
INPUT = 'input'
MODEL = 'model'
