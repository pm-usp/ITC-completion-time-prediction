# ### Importing bibs

import os
import math
import json
import numpy as np
import pandas as pd
from scipy import sparse


#### Functions

##### Reading data

def get_matrices_filenames(path):
    res = {}
    filenames = os.listdir(path)
    for filename in filenames:
        if ('.csv' in filename or '.npz' in filename) and '._' not in filename:
            res[filename.replace('.csv','').replace('.npz','')] = filename
    return res

def read_matrix(filename,index_col=None):
    if '.csv' in filename:
        return pd.read_csv(filename, index_col=index_col), None, None
    # it is a crs matrix, so we will also need the columns and index
    cols_f = open('%s-cols.json'%filename.replace('.npz',''),'r')
    index_f = open('%s-rows.json'%filename.replace('.npz',''),'r')
    return sparse.load_npz(filename), json.load(cols_f), json.load(index_f)


##### Creating labels
# Labels are created looking to the binary matrix

def check_awaitings(x):
    if (x['incident_state-Awaiting Problem'] == 1 or 
        x['incident_state-Awaiting User Info'] == 1 or
        x['incident_state-Awaiting Vendor'] == 1 or
        x['incident_state-Awaiting Evidence'] == 1):
        return 1
    return 0


def check_easy_categories(x):
    if (x['category-Passwords and access'] == 1 or 
        x['category-Remote access'] == 1 or
        x['category-Office pack'] == 1):
        return 1
    return 0


def check_priority1(x):
    if x['priority-1 - Critical'] == 1:
        return 1
    return 0


def check_priority2(x):
    if x['priority-2 - High'] == 1:
        return 1
    return 0
        

def check_priority3(x):
    if x['priority-3 - Moderate'] == 1:
        return 1
    return 0
        

def check_priority4(x):
    if x['priority-4 - Low'] == 1:
        return 1
    return 0


##### Checking original log
log = pd.read_csv('.log/incident_evt_log-processed1-withdurations.csv')

matrices_path = 'matrices/'
matrices = get_matrices_filenames(matrices_path)

# Labels are created looking to the binary matrix
bin_matrix_filename = 'individual_specialist_binary.csv'
bin_matrix = read_matrix(matrices_path+bin_matrix_filename)[0]

bin_matrix['label-Awaitings'] = bin_matrix.apply(lambda x: check_awaitings(x), axis=1)
bin_matrix['label-Easy_categs'] = bin_matrix.apply(lambda x: check_easy_categories(x), axis=1)
bin_matrix['label-Priority1'] = bin_matrix.apply(lambda x: check_priority1(x), axis=1)
bin_matrix['label-Priority2'] = bin_matrix.apply(lambda x: check_priority2(x), axis=1)
bin_matrix['label-Priority3'] = bin_matrix.apply(lambda x: check_priority3(x), axis=1)
bin_matrix['label-Priority4'] = bin_matrix.apply(lambda x: check_priority4(x), axis=1)
labels_cols = [col for col in bin_matrix.columns if 'label' in col]
labeled_matrix = bin_matrix[['number'] + labels_cols].set_index('number')


labeled_matrix[labels_cols].to_csv(matrices_path+'labeled/individual_specialist.csv')


#### Constrains

##### Defining auxiliar functions


def get_mls(indexes_list, ml):
    count = 0
    for indexes in indexes_list:
        for i in indexes:
            copy = indexes.copy()
            copy.remove(i)
            ml[i] = ml[i].union(copy)
            count += len(ml[i])
    return count, ml


def get_cls(indexes_pairs, cl):
    count = 0
    for indexes_pair in indexes_pairs:
        indexes1, indexes2 = indexes_pair
        for index1 in indexes1:
            for index2 in indexes2:
                cl[index1].add(index2)
                cl[index2].add(index1)
                count+=2
    return count, cl


def get_grouped(labeled_matrix):
    cols = list(labeled_matrix.columns)
    del(cols[0])
    return pd.DataFrame(labeled_matrix.groupby(by=cols)['index'].apply(list)).reset_index()

def apply_slicing(indexes, constrains_unique_perc, constrains_rep_count, constrains_rep_perc):
    # INCLUIR SHUFFLE!!!
    max_unique_index = math.ceil(len(indexes)*math.sqrt(constrains_unique_perc))
    if constrains_rep_perc:
        indexes = set(np.concatenate(indexes[:max_unique_index]
                                     .apply(lambda x: x[:math.ceil(len(x)*constrains_rep_perc)]).values, axis=0))
    else:
        indexes = set(np.concatenate(indexes[:max_unique_index]
                                     .apply(lambda x: x[:constrains_rep_count]).values, axis=0))   
    return indexes


def save_constrains(ml, cl, path_constrains):
    
    path_dir = '/'.join(path_constrains.split('/')[:-1])
    if not os.path.exists(path_dir):
        os.makedirs(path_dir)
        
    with open(path_constrains + '-ml.json', 'w') as f:
        ml = {key: list(map(int,ml[key])) for key in ml}
        json.dump(ml,f)
    with open(path_constrains + '-cl.json', 'w') as f:
        cl = {key: list(map(int,cl[key])) for key in cl}
        json.dump(cl,f)


##### ML entre "Awaiting..."s


def get_awaitingsml(labeled_dataset,constrains_unique_perc = 1, constrains_rep_count = None, 
                    constrains_rep_perc = None, path_constrains = None):
    
    grouped = get_grouped(labeled_dataset)
    
    #select indexes lists
    awaitings_indexes = apply_slicing(grouped.loc[grouped['label-Awaitings']==1]['index'],
                                     constrains_unique_perc, constrains_rep_count, constrains_rep_perc)
    count_datapoints = len(awaitings_indexes)
    
    #inicialize ml cl lists
    ml = {i:set() for i in labeled_dataset.index}
    cl = {i:set() for i in labeled_dataset.index}
    
    #get ml and cl dicts
    count_mls, ml = get_mls([awaitings_indexes], ml)
    count_constrains = count_mls 
    
    #save constrains
    if path_constrains:
        save_constrains(ml, cl, path_constrains)
    
    print(constrains_unique_perc, constrains_rep_count, constrains_rep_perc, count_datapoints, count_constrains)
    return count_datapoints, count_constrains


##### ML entre categorias de incidentes de resolução "fácil"


def get_easycategs(labeled_dataset, constrains_unique_perc = 1, constrains_rep_count = None, 
                    constrains_rep_perc = None, path_constrains = None):
    
    grouped = get_grouped(labeled_dataset)
    
    #select indexes lists
    awaitings_indexes = apply_slicing(grouped.loc[grouped['label-Easy_categs']==1]['index'],
                                     constrains_unique_perc, constrains_rep_count, constrains_rep_perc)
    count_datapoints = len(awaitings_indexes)
    
    #inicialize ml cl lists
    ml = {i:set() for i in labeled_dataset.index}
    cl = {i:set() for i in labeled_dataset.index}
    
    #get ml and cl dicts
    count_mls, ml = get_mls([awaitings_indexes], ml)
    count_constrains = count_mls 
    
    #save constrains
    if path_constrains:
        save_constrains(ml, cl, path_constrains)
    
    print(constrains_unique_perc, constrains_rep_count, constrains_rep_perc, count_datapoints, count_constrains)
    return count_datapoints, count_constrains


##### CL entre prioridades 1,2 x 3 x 4


def get_priority12x3x4(labeled_dataset, constrains_unique_perc = 1, constrains_rep_count = None, 
                    constrains_rep_perc = None, path_constrains = None):
    
    grouped = get_grouped(labeled_dataset)
    
    #select indexes lists
    priority12 = apply_slicing(grouped.loc[((grouped['label-Priority1']==1)|
                                            (grouped['label-Priority2']==1)) &
                                           (grouped['label-Priority3']==0) &
                                           (grouped['label-Priority4']==0)]['index'],
                              constrains_unique_perc, constrains_rep_count, constrains_rep_perc)
    
    priority3 = apply_slicing(grouped.loc[(grouped['label-Priority3']==1) &
                                          (grouped['label-Priority1']==0) &
                                          (grouped['label-Priority2']==0) &
                                          (grouped['label-Priority4']==0)]['index'],
                              constrains_unique_perc, constrains_rep_count, constrains_rep_perc)
    
    priority4 = apply_slicing(grouped.loc[(grouped['label-Priority4']==1) &
                                          (grouped['label-Priority1']==0) &
                                          (grouped['label-Priority2']==0) &
                                          (grouped['label-Priority3']==0)]['index'],
                              constrains_unique_perc, constrains_rep_count, constrains_rep_perc)
    
    count_datapoints = len(priority12) + len(priority3) + len(priority4)
    
    #inicialize ml cl lists
    ml = {i:set() for i in labeled_dataset.index}
    cl = {i:set() for i in labeled_dataset.index}
    
    #get ml and cl dicts
    count_cls, cl = get_cls([(priority12, priority3), (priority12, priority4), (priority3, priority4)], cl)
    count_constrains = count_cls 
    
    #save constrains
    if path_constrains:
        save_constrains(ml, cl, path_constrains)
    
    print(constrains_unique_perc, constrains_rep_count, constrains_rep_perc, count_datapoints, count_constrains)
    return count_datapoints, count_constrains


# #### Create Constrains


matrices = {matrix: matrices[matrix] for matrix in matrices if 'specialist' in matrix}
index_col = 'number'

constrains_path = 'cop_results/constrains/'
constrains_types = {'awaitingsml': get_awaitingsml, 'easycategs': get_easycategs, 'priority12x3x4': get_priority12x3x4}
constrains_unique_percs = [0.10, 0.25, 0.50, 0.75, 1.0]
constrains_rep_percs = [0.10, 0.25, 0.50, 0.75, 1.0]

metadata = []
for matrix_name in matrices:
    print(matrix_name)
    matrix = read_matrix(matrices_path+matrices[matrix_name], index_col = index_col)[0]
    matrix = matrix.join(labeled_matrix).reset_index(drop=True).reset_index()
    for constrains_type in constrains_types:
        print(constrains_type)
        for constrains_unique_perc in constrains_unique_percs:
            for constrains_rep_perc in constrains_rep_percs:
                qtt_data, qtt_const = constrains_types[constrains_type](
                                      matrix,
                                      constrains_unique_perc = constrains_unique_perc, 
                                      constrains_rep_perc = constrains_rep_perc,
                                      path_constrains = '%s%s-%s_%sunique_%srep'
                                                         %(constrains_path, 
                                                           matrix_name,
                                                           constrains_type, 
                                                           str(constrains_unique_perc), 
                                                           str(constrains_rep_perc)
                                                          )
                                      )
                metadata.append([matrix,
                                 constrains_type, 
                                 constrains_unique_perc, constrains_rep_perc, 
                                 qtt_data, qtt_const])