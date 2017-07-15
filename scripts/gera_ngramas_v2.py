import logging
import time
import string

import re
from nltk.util import ngrams
import nltk.data
import banco
from nltk.corpus import stopwords
from collections import Counter

conn, cur = banco.conecta_banco()

pt_stopwords = stopwords.words('portuguese')
pt_stopwords.extend(['é', 'ser', 'pra', 'vai', 'vem', '&', '...', 'aí', '%', '-', 'ºC', 'pro'])
string.punctuation = string.punctuation.translate(str.maketrans('', '', '\'!#$%&-./:;=?@\^~,'))
tokenizer = nltk.data.load('tokenizers/punkt/portuguese.pickle')

sql_get_noticias = 'SELECT `idnoticia`, `titulo`, `subtitulo`, `conteudo` FROM noticias WHERE `termoscapturados` = 0'

sql_insert_noticia = 'INSERT INTO noticia_termos (`idnoticia`, `termo`, `frequencia`) VALUES (%s, %s, %s)'

sql_update_noticias = 'UPDATE noticias SET `termoscapturados` = 1 WHERE `idnoticia` = (%s)'

sql_get_facebook_posts = 'SELECT `idpost`, `postagem` FROM fb_posts WHERE `termoscapturados` = 0'

sql_insert_facebook_post = 'INSERT INTO fb_post_termos (`idpost`, `termo`, `frequencia`) VALUES (%s, %s, %s)'

sql_update_facebook_post = 'UPDATE fb_posts SET `termoscapturados` = 1 WHERE `idpost` = (%s)'

sql_get_facebook_comments = 'SELECT * from fb_comentarios WHERE `termoscapturados` = 0'

sql_insert_facebook_comment = 'INSERT INTO fb_comentarios_termos (`idcomentario`, `termo`, `frequencia`) ' \
                             'VALUES (%s, %s, %s)'

sql_update_facebook_comment = 'UPDATE fb_comentarios SET `termoscapturados` = 1 WHERE `idcomentario` = (%s)'

sql_get_twitter_posts = 'SELECT `idpost`, `postagem` FROM tw_posts WHERE `termoscapturados` = 0'

sql_insert_twitter_post = 'INSERT INTO tw_post_termos (`idpost`, `termo`, `frequencia`) VALUES (%s, %s, %s)'

sql_update_twitter_post = 'UPDATE tw_posts SET `termoscapturados` = 1 WHERE `idpost` = (%s)'

sql_get_twitter_mencoes = 'SELECT `idmencao`, `postagem` FROM tw_mencoes WHERE `termoscapturados` = 0'

sql_insert_twitter_mencao = 'INSERT INTO tw_mencao_termos (`idmencao`, `termo`, `frequencia`) VALUES (%s, %s, %s)'

sql_update_twitter_mencao = 'UPDATE tw_mencoes SET `termoscapturados` = 1 WHERE `idmencao` = (%s)'

sql_get_insta_posts = 'SELECT `idpost`, `postagem` FROM insta_posts WHERE `termoscapturados` = 0'

sql_insert_insta_post = 'INSERT INTO insta_post_termos (`idpost`, `termo`, `frequencia`) VALUES (%s, %s, %s)'

sql_update_insta_post = 'UPDATE insta_posts SET `termoscapturados` = 1 WHERE `idpost` = (%s)'

sql_get_insta_comments = 'SELECT `idcomentario`, `postagem` FROM insta_comentarios WHERE `termoscapturados` = 0'

sql_insert_insta_comment = 'INSERT INTO insta_comentario_termos (`idcomentario`, `termo`, `frequencia`)' \
                           'VALUES (%s, %s, %s)'

sql_update_insta_comment = 'UPDATE insta_comentarios SET `termoscapturados` = 1 WHERE `idcomentario` = (%s)'

sql_get_yt_videos = 'SELECT `idvideo`, `titulo`, `descricao` FROM yt_videos WHERE `termoscapturados` = 0'

sql_insert_yt_video = 'INSERT INTO yt_video_termos (`idvideo`, `termo`, `frequencia`) VALUES (%s, %s, %s)'

sql_update_yt_video = 'UPDATE yt_videos SET `termoscapturados` = 1 WHERE `idvideo` = (%s)'

sql_get_yt_comments = 'SELECT `idcomentario`, `postagem` FROM yt_comentarios WHERE `termoscapturados` = 0'

sql_insert_yt_comment = 'INSERT INTO yt_comentario_termos (`idcomentario`, `termo`, `frequencia`)' \
                        'VALUES (%s, %s, %s)'

sql_update_yt_comment = 'UPDATE yt_comentarios SET `termoscapturados` = 1 WHERE `idcomentario` = (%s)'


def get_ngrams_texto_v1(texto):
    texto = texto.replace('\n', '. ').replace(',', ' ').replace('..', '.').replace('!.', '.').replace('?.', '.'). \
        replace(':.', '.').replace('- ', ' ').replace(' -', ' ').replace(' ‘', ' ').replace('’ ', ' '). \
        replace('\' ', ' ').replace(' \'', ' ').replace('/ ', ' ').replace(' /', ' ').replace('\\ ', ' '). \
        replace(' \\', ' ')

    for c in u'.,+*(){}[];:?!<>="”“\/–—·•▫●':
        texto = texto.replace(c, " ")

    lista_texto = texto.split()

    lista_frases = []
    for frase in lista_texto:
        if frase[-1] == '.':
            lista_frases.append(frase[:-1].lower())
        else:
            lista_frases.append(frase[:].lower())

    unigram_list = []
    bigram_list = []
    trigram_list = []
    fourgram_list = []

    for texto in lista_frases:
        texto = texto.replace(':', '').replace(';', '').replace('!', '').replace('?', '')
        ltermos = texto.split()
        unigrams = ltermos
        bigrams = list(ngrams(ltermos, 2))
        trigrams = list(ngrams(ltermos, 3))
        fourgrams = list(ngrams(ltermos, 4))

        for unigram in unigrams:
            if len(unigram) < 3 and len(unigram.encode('utf8')) < 3:
                continue
            if unigram not in pt_stopwords:
                unigram_list.append(unigram)

        for index in range(len(bigrams)):
            if not (bigrams[index][0] in pt_stopwords or bigrams[index][-1] in pt_stopwords):
                bigram_list.append(bigrams[index])

        for index in range(len(trigrams)):
            if not (trigrams[index][0] in pt_stopwords or trigrams[index][-1] in pt_stopwords):
                trigram_list.append(trigrams[index])

        for index in range(len(fourgrams)):
            if not (fourgrams[index][0] in pt_stopwords or fourgrams[index][-1] in pt_stopwords):
                fourgram_list.append(fourgrams[index])

    return unigram_list, bigram_list, trigram_list, fourgram_list


def get_ngrams_texto_v2(texto):

    texto = texto.replace('\n', '. ').replace(',', ' ').replace('..', '.').replace('!.', '.').replace('?.', '.').\
        replace(':.', '.').replace('- ', ' ').replace(' -', ' ').replace(' ‘', ' ').replace('’ ', ' ').\
        replace('\' ', ' ').replace(' \'', ' ').replace('/ ', ' ').replace(' /', ' ').replace('\\ ', ' ').\
        replace(' \\', ' ')
    texto = texto.translate(str.maketrans('', '', string.punctuation + '”“–—·•▫●○■…�›»§®©'))

    lista_texto = tokenizer.tokenize(texto)

    lista_frases = []
    for frase in lista_texto:
        if frase[-1] == '.':
            lista_frases.append(frase[:-1].lower())
        else:
            lista_frases.append(frase[:].lower())

    unigram_list = []
    bigram_list = []
    trigram_list = []
    fourgram_list = []

    for texto in lista_frases:
        texto = texto.replace(':', '').replace(';', '').replace('!', '').replace('?', '')
        ltermos = texto.split()
        unigrams = ltermos
        bigrams = list(ngrams(ltermos, 2))
        trigrams = list(ngrams(ltermos, 3))
        fourgrams = list(ngrams(ltermos, 4))

        for unigram in unigrams:
            if len(unigram) < 3 and len(unigram.encode('utf8')) < 3:
                continue
            if unigram not in pt_stopwords:
                unigram_list.append(unigram)

        for index in range(len(bigrams)):
            if not (bigrams[index][0] in pt_stopwords or bigrams[index][-1] in pt_stopwords):
                bigram_list.append(bigrams[index])

        for index in range(len(trigrams)):
            if not (trigrams[index][0] in pt_stopwords or trigrams[index][-1] in pt_stopwords):
                trigram_list.append(trigrams[index])

        for index in range(len(fourgrams)):
            if not (fourgrams[index][0] in pt_stopwords or fourgrams[index][-1] in pt_stopwords):
                fourgram_list.append(fourgrams[index])

    return unigram_list, bigram_list, trigram_list, fourgram_list


def get_ngramas_noticia(noticia):
    idnoticia = noticia['idnoticia']
    logging.info('Iniciando a captura de ngramas da notícia %d', idnoticia)

    titulo = noticia['titulo'] if noticia['titulo'] else None
    subtitulo = noticia['subtitulo'] if noticia['subtitulo'] else None
    conteudo = noticia['conteudo'] if noticia['conteudo'] else None

    lista_strings = [titulo, subtitulo, conteudo]
    texto = '\n'.join(filter(None, lista_strings))

    unigram_list, bigram_list, trigram_list, fourgram_list = get_ngrams_texto_v2(texto)

    unigram_list = dict(Counter(unigram_list))
    for termo in unigram_list:
        frequencia = unigram_list[termo]
        try:
            cur.execute(sql_insert_noticia, (idnoticia, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    bigram_list = dict(Counter(bigram_list))
    for chave in bigram_list:
        termo = chave[0] + ' ' + chave[1]
        frequencia = bigram_list[chave]
        try:
            cur.execute(sql_insert_noticia, (idnoticia, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    trigram_list = dict(Counter(trigram_list))
    for chave in trigram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2]
        frequencia = trigram_list[chave]
        try:
            cur.execute(sql_insert_noticia, (idnoticia, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    fourgram_list = dict(Counter(fourgram_list))
    for chave in fourgram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2] + ' ' + chave[3]
        frequencia = fourgram_list[chave]
        try:
            cur.execute(sql_insert_noticia, (idnoticia, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    cur.execute(sql_update_noticias, idnoticia)
    conn.commit()


def get_ngramas_noticias():
    cur.execute(sql_get_noticias)
    results = cur.fetchall()
    for noticia in results:
        get_ngramas_noticia(noticia)


def get_ngramas_facebook_post(post):
    idpost = post['idpost']
    logging.info('Iniciando a captura de ngramas do post %d', idpost)

    if post['postagem']:
        texto = post['postagem']
    else:
        logging.info('Post sem texto.')
        cur.execute(sql_update_facebook_post, idpost)
        conn.commit()
        return

    unigram_list, bigram_list, trigram_list, fourgram_list = get_ngrams_texto_v2(texto)
    unigram_list = dict(Counter(unigram_list))
    for termo in unigram_list:
        frequencia = unigram_list[termo]
        try:
            cur.execute(sql_insert_facebook_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    bigram_list = dict(Counter(bigram_list))
    for chave in bigram_list:
        termo = chave[0] + ' ' + chave[1]
        frequencia = bigram_list[chave]
        try:
            cur.execute(sql_insert_facebook_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    trigram_list = dict(Counter(trigram_list))
    for chave in trigram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2]
        frequencia = trigram_list[chave]
        try:
            cur.execute(sql_insert_facebook_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    fourgram_list = dict(Counter(fourgram_list))
    for chave in fourgram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2] + ' ' + chave[3]
        frequencia = fourgram_list[chave]
        try:
            cur.execute(sql_insert_facebook_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    cur.execute(sql_update_facebook_post, idpost)
    conn.commit()


def get_ngramas_facebook_posts():
    cur.execute(sql_get_facebook_posts)
    results = cur.fetchall()
    for post in results:
        get_ngramas_facebook_post(post)


def get_ngramas_facebook_comment(comment):
    idcomentario = comment['idcomentario']
    logging.info('Iniciando a captura de ngramas do comentario %d', idcomentario)

    if comment['postagem']:
        texto = comment['postagem'].lower()
    else:
        logging.info('Comentário sem texto.')
        cur.execute(sql_update_facebook_comment, idcomentario)
        conn.commit()
        return

    unigram_list, bigram_list, trigram_list, fourgram_list = get_ngrams_texto_v1(texto)

    unigram_list = dict(Counter(unigram_list))
    for termo in unigram_list:
        frequencia = unigram_list[termo]
        try:
            cur.execute(sql_insert_facebook_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    bigram_list = dict(Counter(bigram_list))
    for chave in bigram_list:
        termo = chave[0] + ' ' + chave[1]
        frequencia = bigram_list[chave]
        try:
            cur.execute(sql_insert_facebook_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    trigram_list = dict(Counter(trigram_list))
    for chave in trigram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2]
        frequencia = trigram_list[chave]
        try:
            cur.execute(sql_insert_facebook_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    fourgram_list = dict(Counter(fourgram_list))
    for chave in fourgram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2] + ' ' + chave[3]
        frequencia = fourgram_list[chave]
        try:
            cur.execute(sql_insert_facebook_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    cur.execute(sql_update_facebook_comment, idcomentario)
    conn.commit()


def get_ngramas_facebook_comments():
    logging.info('Iniciando a captura de termos de comentários do Facebook.')
    cur.execute(sql_get_facebook_comments)
    results = cur.fetchall()
    for comment in results:
        get_ngramas_facebook_comment(comment)


def get_ngramas_twitter_post(post):
    idpost = post['idpost']
    logging.info('Iniciando a captura de ngramas do post %d', idpost)

    if post['postagem']:
        texto = post['postagem'].lower()
    else:
        logging.info('Post sem texto.')
        cur.execute(sql_update_twitter_post, idpost)
        conn.commit()
        return

    unigram_list, bigram_list, trigram_list, fourgram_list = get_ngrams_texto_v2(texto)

    unigram_list = dict(Counter(unigram_list))
    for termo in unigram_list:
        frequencia = unigram_list[termo]
        try:
            cur.execute(sql_insert_twitter_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    bigram_list = dict(Counter(bigram_list))
    for chave in bigram_list:
        termo = chave[0] + ' ' + chave[1]
        frequencia = bigram_list[chave]
        try:
            cur.execute(sql_insert_twitter_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    trigram_list = dict(Counter(trigram_list))
    for chave in trigram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2]
        frequencia = trigram_list[chave]
        try:
            cur.execute(sql_insert_twitter_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    fourgram_list = dict(Counter(fourgram_list))
    for chave in fourgram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2] + ' ' + chave[3]
        frequencia = fourgram_list[chave]
        try:
            cur.execute(sql_insert_twitter_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    cur.execute(sql_update_twitter_post, idpost)
    conn.commit()


def get_ngramas_twitter_posts():
    logging.info('Iniciando a captura dos termos dos posts do Twitter')
    cur.execute(sql_get_twitter_posts)
    results = cur.fetchall()
    for post in results:
        get_ngramas_twitter_post(post)


def get_ngramas_twitter_mencao(post):
    idpost = post['idmencao']
    logging.info('Iniciando a captura de ngramas da mencao %d', idpost)

    if post['postagem']:
        texto = post['postagem'].lower()
    else:
        logging.info('Menção sem texto.')
        cur.execute(sql_update_twitter_mencao, idpost)
        conn.commit()
        return

    unigram_list, bigram_list, trigram_list, fourgram_list = get_ngrams_texto_v1(texto)

    unigram_list = dict(Counter(unigram_list))
    for termo in unigram_list:
        frequencia = unigram_list[termo]
        try:
            cur.execute(sql_insert_twitter_mencao, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    bigram_list = dict(Counter(bigram_list))
    for chave in bigram_list:
        termo = chave[0] + ' ' + chave[1]
        frequencia = bigram_list[chave]
        try:
            cur.execute(sql_insert_twitter_mencao, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    trigram_list = dict(Counter(trigram_list))
    for chave in trigram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2]
        frequencia = trigram_list[chave]
        try:
            cur.execute(sql_insert_twitter_mencao, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    fourgram_list = dict(Counter(fourgram_list))
    for chave in fourgram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2] + ' ' + chave[3]
        frequencia = fourgram_list[chave]
        try:
            cur.execute(sql_insert_twitter_mencao, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    cur.execute(sql_update_twitter_mencao, idpost)
    conn.commit()


def get_ngramas_twitter_mencoes():
    logging.info('Iniciando a captura dos termos das menções do Twitter')
    cur.execute(sql_get_twitter_mencoes)
    results = cur.fetchall()
    for post in results:
        get_ngramas_twitter_mencao(post)


def get_ngramas_insta_post(post):
    idpost = post['idpost']
    logging.info('Iniciando a captura de ngramas do post %d', idpost)

    if post['postagem']:
        texto = post['postagem'].lower()
    else:
        logging.info('Post sem texto.')
        cur.execute(sql_update_insta_post, idpost)
        conn.commit()
        return

    unigram_list, bigram_list, trigram_list, fourgram_list = get_ngrams_texto_v2(texto)

    unigram_list = dict(Counter(unigram_list))
    for termo in unigram_list:
        frequencia = unigram_list[termo]
        try:
            cur.execute(sql_insert_insta_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    bigram_list = dict(Counter(bigram_list))
    for chave in bigram_list:
        termo = chave[0] + ' ' + chave[1]
        frequencia = bigram_list[chave]
        try:
            cur.execute(sql_insert_insta_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    trigram_list = dict(Counter(trigram_list))
    for chave in trigram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2]
        frequencia = trigram_list[chave]
        try:
            cur.execute(sql_insert_insta_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    fourgram_list = dict(Counter(fourgram_list))
    for chave in fourgram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2] + ' ' + chave[3]
        frequencia = fourgram_list[chave]
        try:
            cur.execute(sql_insert_insta_post, (idpost, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    cur.execute(sql_update_insta_post, idpost)
    conn.commit()


def get_ngramas_insta_posts():
    logging.info('Iniciando a captura dos termos dos posts do Instagram.')
    cur.execute(sql_get_insta_posts)
    results = cur.fetchall()
    for post in results:
        get_ngramas_insta_post(post)


def get_ngramas_insta_comment(comment):
    idcomentario = comment['idcomentario']
    logging.info('Iniciando a captura de ngramas do comentario %d', idcomentario)

    if comment['postagem']:
        texto = comment['postagem'].lower()
    else:
        logging.info('Comentário sem texto.')
        cur.execute(sql_update_insta_comment, idcomentario)
        conn.commit()
        return

    unigram_list, bigram_list, trigram_list, fourgram_list = get_ngrams_texto_v1(texto)

    unigram_list = dict(Counter(unigram_list))
    for termo in unigram_list:
        frequencia = unigram_list[termo]
        try:
            cur.execute(sql_insert_insta_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    bigram_list = dict(Counter(bigram_list))
    for chave in bigram_list:
        termo = chave[0] + ' ' + chave[1]
        frequencia = bigram_list[chave]
        try:
            cur.execute(sql_insert_insta_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    trigram_list = dict(Counter(trigram_list))
    for chave in trigram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2]
        frequencia = trigram_list[chave]
        try:
            cur.execute(sql_insert_insta_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    fourgram_list = dict(Counter(fourgram_list))
    for chave in fourgram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2] + ' ' + chave[3]
        frequencia = fourgram_list[chave]
        try:
            cur.execute(sql_insert_insta_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    cur.execute(sql_update_insta_comment, idcomentario)
    conn.commit()


def get_ngramas_insta_comments():
    logging.info('Iniciando a captura de termos de comentários do Instagram.')
    cur.execute(sql_get_insta_comments)
    results = cur.fetchall()
    for comment in results:
        get_ngramas_insta_comment(comment)


def get_ngramas_yt_video(video):
    idvideo = video['idvideo']
    logging.info('Iniciando a captura de ngramas do video %d', idvideo)

    texto = video['titulo'].lower()
    if video['descricao']:
        texto = texto + ' ' + video['descricao'].lower()

    unigram_list, bigram_list, trigram_list, fourgram_list = get_ngrams_texto_v2(texto)

    unigram_list = dict(Counter(unigram_list))
    for termo in unigram_list:
        frequencia = unigram_list[termo]
        try:
            cur.execute(sql_insert_yt_video, (idvideo, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    bigram_list = dict(Counter(bigram_list))
    for chave in bigram_list:
        termo = chave[0] + ' ' + chave[1]
        frequencia = bigram_list[chave]
        try:
            cur.execute(sql_insert_yt_video, (idvideo, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    trigram_list = dict(Counter(trigram_list))
    for chave in trigram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2]
        frequencia = trigram_list[chave]
        try:
            cur.execute(sql_insert_yt_video, (idvideo, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    fourgram_list = dict(Counter(fourgram_list))
    for chave in fourgram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2] + ' ' + chave[3]
        frequencia = fourgram_list[chave]
        try:
            cur.execute(sql_insert_yt_video, (idvideo, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    cur.execute(sql_update_yt_video, idvideo)
    conn.commit()


def get_ngramas_yt_videos():
    logging.info('Iniciando a captura dos termos dos vídeos do Youtube.')
    cur.execute(sql_get_yt_videos)
    results = cur.fetchall()
    for video in results:
        get_ngramas_yt_video(video)


def get_ngramas_yt_comment(comment):
    idcomentario = comment['idcomentario']
    logging.info('Iniciando a captura de ngramas do comentario %d', idcomentario)

    if comment['postagem']:
        texto = comment['postagem'].lower()
    else:
        logging.info('Comentário sem texto.')
        cur.execute(sql_update_yt_comment, idcomentario)
        conn.commit()
        return

    unigram_list, bigram_list, trigram_list, fourgram_list = get_ngrams_texto_v1(texto)

    unigram_list = dict(Counter(unigram_list))
    for termo in unigram_list:
        frequencia = unigram_list[termo]
        try:
            cur.execute(sql_insert_yt_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    bigram_list = dict(Counter(bigram_list))
    for chave in bigram_list:
        termo = chave[0] + ' ' + chave[1]
        frequencia = bigram_list[chave]
        try:
            cur.execute(sql_insert_yt_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    trigram_list = dict(Counter(trigram_list))
    for chave in trigram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2]
        frequencia = trigram_list[chave]
        try:
            cur.execute(sql_insert_yt_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    fourgram_list = dict(Counter(fourgram_list))
    for chave in fourgram_list:
        termo = chave[0] + ' ' + chave[1] + ' ' + chave[2] + ' ' + chave[3]
        frequencia = fourgram_list[chave]
        try:
            cur.execute(sql_insert_yt_comment, (idcomentario, termo, frequencia))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()

    cur.execute(sql_update_yt_comment, idcomentario)
    conn.commit()


def get_ngramas_yt_comments():
    logging.info('Iniciando a captura de termos de comentários do Youtube.')
    cur.execute(sql_get_yt_comments)
    results = cur.fetchall()
    for comment in results:
        get_ngramas_yt_comment(comment)


start_time = time.time()
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
get_ngramas_noticias()
get_ngramas_facebook_posts()
get_ngramas_facebook_comments()
get_ngramas_twitter_mencoes()
get_ngramas_twitter_posts()
get_ngramas_insta_posts()
get_ngramas_insta_comments()
get_ngramas_yt_videos()
get_ngramas_yt_comments()
cur.close()
conn.close()

logging.info("Tempo de execução do script: %s segundos" % (time.time() - start_time))
logging.info("Script executado com sucesso!")
