


def create_new_group(new_df_s, new_df_dev, new_df_user):
    """生成新群"""
    global new_group_id
    global old_df
    global result_df

    new_df_date = new_df_s.log_date.values[0]

    # 该dev已有的用户
    old_df_s = old_df[old_df.dev == new_df_dev]
    old_df_user = old_df_s.userid.values[0]

    # old的记录变更
    new_df_s.group_id = new_group_id
    old_df = old_df.append(new_df_s, ignore_index=True)
    old_df.loc[old_df[old_df.userid == old_df_user].index.tolist(), 'group_id'] = new_group_id

    # result记录增加
    new_df_s[['start_date', 'end_date', 'eff_userid', 'eff_dev']] = \
        [new_df_date, '2035-12-31', new_df_user, new_df_dev]
    old_df_s[['group_id', 'start_date', 'end_date', 'eff_userid', 'eff_dev']] = \
        [new_group_id, new_df_date, '2035-12-31', new_df_user, new_df_dev]

    result_df = result_df.append(new_df_s, ignore_index=True).append(old_df_s, ignore_index=True)

