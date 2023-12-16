import flask
import logging
import psycopg2
import string
import random
import jwt

app = flask.Flask(__name__)

web_tokens = []

StatusCodes = {
    'success': 200,
    'api_error': 400,
    'internal_error': 500
}


## CONNECTION TO THE DATABASE:
##################################################################
##  -ACCESSING SERVER THROUGH 'host' AND 'port'
##  -ACCESSING DATABASE WITHIN SERVER THROUGH 'database' AND 'user'
##################################################################

def db_connection():
    db = psycopg2.connect(
        host='127.0.0.1',  # or host='localhost'
        port='5432',
        database='bdprojeto',
        user='marcolucas',
    )
    return db


# USER REGISTRATION
@app.route('/user/', methods=['POST'])
def register_user():
    logger.info('POST /user')
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'POST /user - payload: {payload}')

    size_id = 10  # number of characters in id.
    id = ''.join(random.choices(string.digits, k=size_id))

    try:
        cur.execute('SELECT ID FROM user')
        rows = cur.fetchall()
        for row in rows:
            if row[0] == id:
                id = ''.join(random.choices(string.digits, k=size_id))
        logger.info(f'POST/user - User ID created')


    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /user - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}
        conn.rollback()

    statement = 'INSERT INTO user (ID,username,age,address,email,password) VALUES (%s, %s, %s, %s, %s, %s, %s)'
    values = (id, payload['name'], payload['age'], payload['address'], payload['email'], payload['password'])

    try:

        cur.execute(statement, values)
        conn.commit()
        response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'User ID: {id}'}

    except (Exception, psycopg2.DatabaseError) as error:

        logger.error(f'POST /user - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': f'User ID: Not Obtained'}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


# USER AUTHENTICATION
@app.route('/user/', methods=['PUT'])
def login_user():
    payload = flask.request.get_json()
    name = payload['name']
    password = payload['password']

    conn = db_connection()
    cur = conn.cursor()

    try:
        encoded_jwt = jwt.encode({name: password}, "secret", algorithm="HS256")

        cur.execute('UPDATE user SET token = %s WHERE username = %s AND password = %s', (encoded_jwt, name, password))
        response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'JWT: {id}'}
        web_tokens.append(encoded_jwt)
        # commit the transaction
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['api_error'], 'errors': str(error),
                    'results': 'Login Error: Username or Password Invalid'}

        # an error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


# ADD SONG
@app.route('/songs/', methods=['POST'])
def add_song():
    logger.info('POST /song')
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    size_id = 10  # number of characters in id.
    id = ''.join(random.choices(string.digits, k=size_id))

    try:
        cur.execute('SELECT ISMN FROM songs')
        rows = cur.fetchall()
        for row in rows:
            if row[0] == id:
                id = ''.join(random.choices(string.digits, k=size_id))
        logger.debug(f'POST/user - ISMN created %s', id)
    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}
        conn.rollback()

    statement = 'INSERT INTO songs (ISMN,TITLE,DURATION,RELEASE_DATE,PUBLISHER) VALUES (%s,%s,%s,%s,%s)'
    values = (id, payload['TITLE'], payload['DURATION'], payload['RELEASE_DATE'], payload['PUBLISHER'])

    try:
        cur.execute(statement, values)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'Song ID: {id}'}

    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}

        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


# ADD ALBUM
@app.route('/album/', methods=['POST'])
def add_album():
    logger.info('POST /album')

    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    tk = payload['jwt']

    if tk not in web_tokens:
        response = {'status': StatusCodes['internal_error'], 'errors': 'JWT Invalid', 'results': 'Not Obtainable'}
        conn.rollback()
        conn.close()
    else:
        size_id = 10  # number of characters in id.
        id = ''.join(random.choices(string.digits, k=size_id))

        try:
            cur.execute('SELECT ID FROM album')
            rows = cur.fetchall()
            for row in rows:
                if row[0] == id:
                    id = ''.join(random.choices(string.digits, k=size_id))
            logger.debug(f'POST/user - Album ID created')

        except (Exception, psycopg2.DatabaseError) as error:
            response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}
            conn.rollback()

        statement = 'INSERT INTO album (ID,album_name,release_date,publisher,songs) VALUES (%s,%s,%s,%s,%s)'
        values = (id, payload['album_name'], payload['release_date'], payload['publisher'],
                  payload['songs'])

        try:
            cur.execute(statement, values)
            # commit the transaction
            conn.commit()
            response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'Song ID: {id}'}

        except (Exception, psycopg2.DatabaseError) as error:
            response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}

            conn.rollback()

        finally:
            if conn is not None:
                conn.close()

    return flask.jsonify(response)


# SEARCH SONG
@app.route('/song/<keyword>', methods=['GET'])
def search_song(keyword):
    logger.info('GET /song/<keyword>')
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()


# DETAIL ARTIST
@app.route('/artists/<id>', methods=['GET'])
def list_details_artist(id):
    logger.info('GET /artist/<id>')
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    tk = payload['jwt']
    if tk not in web_tokens:
        response = {'status': StatusCodes['internal_error'], 'errors': 'JWT Invalid', 'results': 'Not Obtainable'}
        conn.rollback()
        conn.close()
    else:
        try:
            cur.execute('SELECT artistic_name, songs, albums,label FROM artist where id = %s', (id,))
            rows = cur.fetchall()
            row = rows[0]
            content = {'name': row[0], 'songs': row[1], 'albums': row[2], 'label': row[3]}
            response = {'status': StatusCodes['success'], 'errors': 'None', 'results': content}

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f'GET /artist/<id> - error: {error}')
            response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}

        finally:
            if conn is not None:
                conn.close()

    return flask.jsonify(response)

# SUBSCRIBE TO PREMIUM
@app.route('/subscription/', methods=['POST'])
def subscribe_to_premium():
    logger.info('POST /subscription/')
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    size_id = 10  # number of characters in id.
    id = ''.join(random.choices(string.digits, k=size_id))

    try:
        cur.execute('SELECT ID_SUBSCRIPTION FROM SUBSCRIPTION_PREMIUM')
        rows = cur.fetchall()
        for row in rows:
            if row[0] == id:
                id = ''.join(random.choices(string.digits, k=size_id))

        logger.debug(f'POST/user - Subscription created %s', id)
    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}
        conn.rollback()

    statement = 'INSERT INTO SUBSCRIPTION_PREMIUM (ID_SUBSCRIPTION,PLAN,PRICE,PREMIUM_START_DATE,PREMIUM_END_DATE) VALUES (%s,%s,%s,%s,%s)'
    values = (id, payload['PLAN'], payload['PRICE'], payload['PREMIUM_START_DATE'], payload['PREMIUM_END_DATE'])

    try:
        cur.execute(statement, values)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'Subscription ID: {id}'}

    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}

        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)

# PLAY SONG
@app.route('/song/<song_id>', methods=['PUT'])
def play_song(song_id):
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()


# LEAVE FEEDBACK
@app.route('/comments/<song_id>/<parent_comment_id>', methods=['POST'])
def leave_feedback(parent_comment_id, song_id):
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    size_id = 10  # number of characters in id.
    id = ''.join(random.choices(string.digits, k=size_id))

    try:
        cur.execute('SELECT ID_COMMENT FROM COMMENTARY Where ID_COMMENT = %s AND ISMN = %s', (parent_comment_id, song_id))
        rows = cur.fetchall()
        for row in rows:
            if row[0] == id:
                id = ''.join(random.choices(string.digits, k=size_id))

        logger.debug(f'POST/user - Reply to commentary created %s', id)
    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}
        conn.rollback()

    statement = 'INSERT INTO COMMENTARY (ID_COMMENT,ID_PLATFORM_USER,ISMN,COMMENT_TEXT,CREATION_DATE) VALUES (%s,%s,%s,%s,%s)'
    values = (id, payload['ID_PLATFORM_USER'], payload['ISMN'], payload['COMMENT_TEXT'], payload['CREATION_DATE'])
    statement2 = 'INSERT INTO COMMENTS_COMMENTS (ID_COMMENT1,ID_COMMENT2) VALUES(%s,%s)
    values2 = (id,parent_commend)

    try:
        cur.execute(statement, values)
        cur.execute(statement2,values2)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'Reply to commentary ID: {id}'}

    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}

        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)

# LEAVE COMMENT
@app.route('/comments/<song_id>', methods=['POST'])
def leave_comment(song_id):
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    size_id = 10  # number of characters in id.
    id = ''.join(random.choices(string.digits, k=size_id))

    try:
        cur.execute('SELECT ID_COMMENT FROM COMMENTARY Where ISMN= %s', (song_id))
        rows = cur.fetchall()
        for row in rows:
            if row[0] == id:
                id = ''.join(random.choices(string.digits, k=size_id))

        logger.debug(f'POST/user - Commentary created %s', id)
    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}
        conn.rollback()

    statement = 'INSERT INTO COMMENTARY (ID_COMMENT,ID_PLATFORM_USER,ISMN,COMMENT_TEXT,CREATION_DATE) VALUES (%s,%s,%s,%s,%s)'
    values = (id, payload['ID_PLATFORM_USER'], payload['ISMN'], payload['COMMENT_TEXT'], payload['CREATION_DATE'])

    try:
        cur.execute(statement, values)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'Commentary ID: {id}'}

    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}

        conn.rollback()

    finally:
        if conn is not None:
            conn.close()
            
    return flask.jsonify(response)



'''
#GENERATE MONTHLY REPORT
@app.route('/report/topN/<N>/<year-month>', methods=['GET'])
def generate_monthly_report(n,time_period):
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()
'''


# ADD LABEL
@app.route('/labels/', methods=['POST'])
def create_label():
    logger.info('POST /labels')
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    size_id = 10  # number of characters in id.
    id = ''.join(random.choices(string.digits, k=size_id))

    try:
        cur.execute('SELECT ID_LABEL FROM LABELS')
        rows = cur.fetchall()
        for row in rows:
            if row[0] == id:
                id = ''.join(random.choices(string.digits, k=size_id))

        logger.debug(f'POST/user - ISMN created %s', id)
    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}
        conn.rollback()

    statement = 'INSERT INTO LABELS (ID_LABEL,NAME) VALUES (%s,%s)'
    values = (id, payload['NAME'])

    try:
        cur.execute(statement, values)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'Label ID: {id}'}

    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}

        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


# ADD ARTIST
@app.route('/artists/', methods=['POST'])
def create_artist():
    logger.info('POST /artists')
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    size_id = 10  # number of characters in id.
    id = ''.join(random.choices(string.digits, k=size_id))

    try:
        cur.execute('SELECT ID_ARTIST FROM ARTISTS')
        rows = cur.fetchall()
        for row in rows:
            if row[0] == id:
                id = ''.join(random.choices(string.digits, k=size_id))

        logger.debug(f'POST/user - Artist ID created %s', id)
    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}
        conn.rollback()

    statement = 'INSERT INTO ARTISTS (ID_ARTIST,NAME,ARTISTIC_NAME,EMAIL,PASSWORD,ADDRESS,AGE,ID_LABEL) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
    values = (id, payload['NAME'], payload['ARTISTIC_NAME'], payload['EMAIL'], payload['PASSWORD'], payload['ADDRESS'],
              payload['AGE'], payload['ID_LABEL'])

    try:
        cur.execute(statement, values)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'Artist ID: {id}'}

    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}

        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


# Creat Playlist
@app.route('/playlist/', methods=['POST'])
def create_playlist():
    logger.info('POST /playlist')
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    size_id = 10  # number of characters in id.
    id = ''.join(random.choices(string.digits, k=size_id))

    try:
        cur.execute('SELECT ID_PLAYLIST FROM PLAYLIST')
        rows = cur.fetchall()
        for row in rows:
            if row[0] == id:
                id = ''.join(random.choices(string.digits, k=size_id))

        logger.debug(f'POST/user - Playlist ID %s created', id)
    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}
        conn.rollback()

    statement = 'INSERT INTO PLAYLIST (ID_PLAYLIST,PLAYLIST_NAME,CREATION_DATE,PLAYLIST_TYPE) VALUES (%s,%s,%s,%s)'
    values = (id, payload['PLAYLIST_NAME'], payload['CREATION_DATE'], payload['PLAYLIST_TYPE'])

    try:
        cur.execute(statement, values)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'Playlist ID: {id}'}

    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}

        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)

#Creat Song Collection
@app.route('/song_collection/', methods=['POST'])
def create_playlist():
    logger.info('POST /song_collection')
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    size_id = 10  # number of characters in id.
    id = ''.join(random.choices(string.digits, k=size_id))

    try:
        cur.execute('SELECT ID_COLLECTION FROM SONG_COLLECTION')
        rows = cur.fetchall()
        for row in rows:
            if row[0] == id:
                id = ''.join(random.choices(string.digits, k=size_id))

        logger.debug(f'POST/user - Song collection ID %s created', id)
    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}
        conn.rollback()

    statement = 'INSERT INTO SONG_COLLECTION (ID_COLLECTION,SONG_POSITION,ISMN,ID_PLAYLIST) VALUES (%s,%s,%s,%s)'
    values = (id, payload['SONG_POSITION'], payload['ISMN'], payload['ID_PLAYLIST'])

    try:
        cur.execute(statement, values)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'Song collection ID: {id}'}

    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}

        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)

#Creat pre paid cards subscription premium
@app.route('/pre_paid_cards_subscription_premium/', methods=['POST'])
def create_playlist():
    logger.info('POST /pre_paid_cards_subscription_premium')
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    size_id = 10  # number of characters in id.
    id = ''.join(random.choices(string.digits, k=size_id))

    try:
        cur.execute('SELECT PRE_PAID_CARDS_SUBSCRIPTION_PREMIUM FROM PRE_PAID_CARDS_SUBSCRIPTION_PREMIUM')
        rows = cur.fetchall()
        for row in rows:
            if row[0] == id:
                id = ''.join(random.choices(string.digits, k=size_id))

        logger.debug(f'POST/user - Pre paid card %s created', id)
    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}
        conn.rollback()

    statement = 'INSERT INTO PRE_PAID_CARDS_SUBSCRIPTION_PREMIUM (ID_CARD,ID_SUBSCRIPTION) VALUES (%s,%s)'
    values = (id, payload['ID_SUBSCRIPTION']) #ATENÇAOOOO

    try:
        cur.execute(statement, values)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'errors': 'None', 'results': f'Pre paid card ID: {id}'}

    except (Exception, psycopg2.DatabaseError) as error:
        response = {'status': StatusCodes['internal_error'], 'errors': str(error), 'results': 'Not Obtainable'}

        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)



if __name__ == '__main__':
    logging.basicConfig(filename='/tmp/' + 'log_file.log')
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s]:  %(message)s', '%H:%M:%S')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    host = '127.0.0.1'
    port = 8080
    app.run(host=host, debug=True, threaded=True, port=port)
    logger.info(f'API v1.0 online: http://{host}:{port}')
