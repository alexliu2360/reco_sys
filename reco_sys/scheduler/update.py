# coding='utf8'


from offline.update_article import UpdateArticle
from offline.update_user import UpdateUserProfile

def update_article_profile():
    '''
    更新文章画像
    :return:
    '''

    ua = UpdateArticle()
    sentence_df = ua.merge_article_data()
    if sentence_df.rdd.collect():
        text_rank_res = ua.generate_article_label(sentence_df)
        article_profile = ua.get_article_profile(text_rank_res)
        ua.compute_article_similar(article_profile)




def update_user_profile():
    """
    更新用户画像
    """
    uup = UpdateUserProfile()
    if uup.update_user_action_basic():
        uup.update_user_label()
        uup.update_user_info()