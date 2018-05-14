#!/usr/bin/env python
# coding=UTF-8

import os
import subprocess
import pandas as pd
import datetime
import sys
from concurrent import futures

# 禁用一些警告
pd.options.mode.chained_assignment = None


# 生成csv文件
def table_to_csv(data_file, table_name):
    # 文件存在则删除
    if os.path.exists(data_file):
        os.remove(data_file)

    sql_cmd = 'hive -e "set hive.cli.print.header=true; \
              select * from %s " >%s' % (table_name, data_file)
    subprocess.call(sql_cmd, shell=True)
    # 替换其中的字段分隔符/t为,
    sed_cmd = 'sed -i "s/\t/,/g" %s' % (data_file)
    subprocess.call(sed_cmd, shell=True)


# dataframe导入hive
def df_to_table(result_df_p, result_csv, table_name, columns_list):
    if os.path.exists(result_csv):
        os.remove(result_csv)

    result_df_p.to_csv(result_csv, index=False, header=None, encoding="utf8", columns=columns_list)

    cmd_hive = '''hive -e "load data local inpath '%s' overwrite into table %s"''' % (result_csv, table_name)
    subprocess.call(cmd_hive, shell=True)


def result_add(new_df_s):
    """结果新增记录"""
    global result_df
    append_df_s = new_df_s.copy()
    append_df_s.start_date = new_df_s.log_date.values[0]
    append_df_s.end_date = '2035-12-31'
    append_df_s.eff_userid = new_df_s.userid.values[0]
    append_df_s.eff_dev = new_df_s.dev.values[0]
    result_df = result_df.append(append_df_s, ignore_index=True)


def group_merge(dev_group_id, user_group_id, new_df_s):
    """合并群"""
    global new_group_id
    global result_df
    exp_userid = new_df_s.userid.values[0]
    exp_dev = new_df_s.dev.values[0]
    new_df_date = new_df_s.log_date.values[0]
    # 需要合并的记录
    change_df = result_df[(result_df.group_id == user_group_id) | (result_df.group_id == dev_group_id)].copy()
    # 原来的记录失效，记录失效时间以及造成失效的用户和关系
    result_df.loc[result_df[(result_df.group_id == user_group_id) | (result_df.group_id \
        == dev_group_id)].index.tolist(), ['end_date', 'exp_userid', 'exp_dev', 'next_group']] = \
        [new_df_date, exp_userid, exp_dev, new_group_id]

    # 生成新记录
    change_df[['group_id', 'start_date', 'end_date', 'eff_userid', 'eff_dev']] = \
        [new_group_id, new_df_date, '2035-12-31', exp_userid, exp_dev]

    result_df = result_df.append(change_df, ignore_index=True)


def change_old_df(dev_group_id, user_group_id, new_df_s):
    """对比已有的绑定关系"""
    global new_group_id
    global old_df
    # 如果用户或绑定关系原来不在同一群组
    if dev_group_id is not None and user_group_id is not None and dev_group_id != user_group_id:
        new_df_s.group_id = new_group_id
        old_df.loc[old_df[(old_df.group_id == user_group_id) | (old_df.group_id == dev_group_id)].index.tolist(), \
                   'group_id'] = new_group_id
        # 两个群合并
        group_merge(dev_group_id, user_group_id, new_df_s)

        new_group_id += 1

    # 如果用户或绑定关系原来没有群组或在同一群组
    else:
        if dev_group_id is None and user_group_id is None:
            # 完全新记录
            new_df_s.group_id = new_group_id
            result_add(new_df_s)
            new_group_id += 1
        elif dev_group_id is None and user_group_id is not None:
            new_df_s.group_id = user_group_id
        elif dev_group_id is not None and user_group_id is None:
            new_df_s.group_id = dev_group_id
            result_add(new_df_s)
        else:
            new_df_s.group_id = dev_group_id

    old_df = old_df.append(new_df_s, ignore_index=True)


def get_change_group(new_df_s, old_df):
    """新记录带来的影响"""
    # 获取新记录的设备和用户
    new_df_dev = new_df_s.dev.values[0]
    new_df_user = new_df_s.userid.values[0]

    # 设备和用户所在的群组
    if not old_df[old_df.dev == new_df_dev].empty:
        dev_group_id = old_df[old_df.dev == new_df_dev].group_id.drop_duplicates().values[0]
    else:
        dev_group_id = None

    if not old_df[old_df.userid == new_df_user].empty:
        user_group_id = old_df[old_df.userid == new_df_user].group_id.drop_duplicates().values[0]
    else:
        user_group_id = None

    return (dev_group_id, user_group_id)


def main():
    # 初始群号
    global new_group_id
    date1 = sys.argv[1]
    d1 = datetime.datetime(2007, 1, 1)
    d2 = datetime.datetime.strptime(date1, "%Y-%m-%d")
    new_group_id = (d2 - d1).days * 1000000

    # 列顺序
    result_columns = ['userid', 'dev', 'log_time', 'log_date', 'seq_id', 'join_type', 'group_id', \
                      'start_date', 'end_date', 'eff_userid', 'eff_dev', 'exp_userid', 'exp_dev', 'next_group']

    # 读入数据
    table_to_csv("test_group_new.csv", "tmpdb.tmp_user_group_new_relation")
    table_to_csv("test_group_old.csv", "tmpdb.tmp_user_group_relation_effected")
    table_to_csv("test_group_result.csv", "tmpdb.tmp_user_group_records_effected")

    global new_df
    global old_df
    global result_df
    new_df = pd.read_csv("test_group_new.csv").sort_values(by=['seq_id'], axis=0, ascending=True)
    old_df = pd.read_csv("test_group_old.csv")
    result_df = pd.read_csv("test_group_result.csv")

    # old_df = pd.DataFrame(columns=['userid', 'dev', 'log_time', 'log_date', 'seq_id', 'join_type', \
    #                                'group_id', 'start_date', 'end_date'])
    # result_df = pd.DataFrame(columns=['userid', 'dev', 'log_time', 'log_date' 'seq_id', 'join_type', \
    #                                   'group_id', 'start_date', 'end_date', 'exp_userid', 'exp_dev', 'next_group'])
    print("Load data to dateframe:", datetime.datetime.now())

    # 逐条处理数据
    for id_t in new_df['seq_id']:
        print(id_t)
        new_df_s = new_df[new_df.seq_id == id_t]
        # 获取用户或绑定关系已存在的群组
        (dev_group_id, user_group_id) = get_change_group(new_df_s, old_df)
        # 老记录群组号变更
        change_old_df(dev_group_id, user_group_id, new_df_s)

    print("Get the result:", datetime.datetime.now())

    # 获取结果
    df_to_table(old_df, "test_group_old_re.csv", "tmpdb.tmp_user_group_relation_effected_re", result_columns)
    df_to_table(result_df, "test_group_result_re.csv", "tmpdb.tmp_user_group_records_effected_re", result_columns)

    print("Finish:", datetime.datetime.now())

    # old_df.to_csv('old.csv', index=False, header=None, encoding="utf8", columns=columns)
    # result_df.to_csv('result.csv', index=False, header=True, encoding="utf8", columns=result_columns)


if __name__ == '__main__':
    main()
