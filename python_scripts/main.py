#!/usr/bin/env python
# -*- coding: utf-8 -*-  
"""

"""
import numpy as np
# from sklearn.ensemble import GradientBoostingClassifier
# from sklearn.ensemble import RandomForestClassifer
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
import pickle

import MySQLdb as mysql


def connect_database(ip='202.120.37.201',user='admin',
    pwd='tianchi',database='result'):

    try:
        conn=mysql.connect(ip,user,pwd,database)
        cur=conn.cursor()
        print "MySQL Connection: Success"
        return [conn,cur]
    except mysql.Error,e:
        print "MySQL Error %d:%s"%(e.args[0],e.args[1])
        return [None,None]

def fetch_data(cur,feature_table_name,table_schema='result',
    except_fields=[('user_id',),('item_id',)]):
    '''
        fetch_data: Get the np.array type from database
        param:
            cur: cursor
            feature_table_name: table name of user-item feature
            table_schema: database name, default is 'tianchi'
            except_fields: Since we only need the feature values except user-id,item-id 
                            record-id, we have to eliminate them from feature fields,
                            default is [('user_id',),('item_id',)]
    '''
    try:
        sql_select_fields='''
            select column_name from information_schema.columns 
            where table_name='%s' and table_schema='%s'
        '''%(feature_table_name,table_schema)
        cur.execute(sql_select_fields)
        feature_fields_all=list(cur.fetchall())
        feature_fields_tuple=list(set(feature_fields_all).difference(set(except_fields)))
        feature_fields=[]
        for field in feature_fields_tuple:
            feature_fields.append(field[0])
        newstr=','.join(feature_fields)
        sql_select_data='''
            select %s from %s
        '''%(newstr,feature_table_name)
        data_count=cur.execute(sql_select_data)
        data_tuples=cur.fetchall()
        real_data=np.array(data_tuples)
        return real_data
    except mysql.Error,e:
        print "MySQL Error %d:%s"%(e.args[0],e.args[1])


def main(args):
    pass
def disconnect_database(conn,cur):
    try:
        cur.close()
        conn.close()
        print "MySQL Disconnection: Success"
    except mysql.Error,e:
        print "MySQL Error %d:%s"%(e.args[0],e.args[1])
    

if __name__ == "__main__":
    #Connect database and get data
    [conn,cur]=connect_database()
    training_data=fetch_data(cur,'train_103_1_9dot6')
    training_groundtruth=fetch_data(cur,'train_ifbuy_1_9dot6',except_fields=[('',)])
    testing_data=fetch_data(cur,'pre_ui_user_item_category')
    print training_data.shape,training_groundtruth.shape
    #Logistic Regression
    logistic_param={'penalty':'l2','dual':False,'class_weight':'auto','max_iter':500,
    'solver':'newton-cg'}
    model_logistic_regression=LogisticRegression(**logistic_param)
    model_logistic_regression.fit(training_data,training_groundtruth)

    testing_result_lr=model_logistic_regression.predict_proba(testing_data)
    testing_result_lr_class=model_logistic_regression.predict(testing_data)
    joblib.dump(testing_result_lr,'lr.model')
    joblib.dump(testing_result_lr_class,'lr_class.model')

    # #Random Forest
    # rand_forest_param={"n_estimators":, "criterion":, "max_depth":}
    # model_rand_forest=RandomForestClassifer(**rand_forest_param)
    # model_rand_forest.fit(training_data,training_groundtruth)

    # testing_result_rf=model_rand_forest.predict_proba(testing_data)



    # #GBRT
    # GBRT_param={"max_depth":,"n_estimators":,"learning_rate":}
    # model_GBRT=GradientBoostingClassifier(**GBRT_param)
    # model_GBRT.fit(training_data,training_groundtruth)

    # testing_result_gbrt=model.predict_proba(testing_data)

    # #For incremental learning with GBRT
    # model_GBRT_inc=GradientBoostingClassifier(warm_start=True)
    # model_GBRT_inc.fit(training_data,training_groundtruth)

    # #Weighted-Decision
    # w_gbrt=0.3;
    # w_lr=0.3;
    # w_rf=0.3;

    # final_result=testing_result_gbrt*w_gbrt+testing_result_rf*w_rf+testing_result_lr*w_lr;

    disconnect_database(conn,cur)