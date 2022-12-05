
#################################### IMPORTS ###########################################

import os
import time
import json
import warnings
import pandas as pd
from tqdm import tqdm
from sklearn.cluster import KMeans
from scripts.clustering_results import (create_metadata_file, get_groups_data, calc_silhouette_per_group,
                                        calc_silhouette, save_clustered_df, save_metadata_incidents)




#################################### FUNCTIONS ###########################################

def get_matrices_filenames(path):
    res = {}
    filenames = os.listdir(path)
    for filename in filenames:
        if ('.csv' in filename or '.npz' in filename) and '._' not in filename:
            res[filename.replace('.csv','').replace('.npz','')] = filename
    return res


def cluster_and_save(log, matrix_filename, duplicate, k, clustering_func, clustering_func_str, run, metadata_filename, index_col, metric_col, 
                     groups_col='group', silhouette_col = 'silhouette'):

    vector, clustering_info, filenames = prepare(matrix_filename, duplicate, k, clustering_func_str, run, metadata_filename, index_col)
    start_time = time.time()
    df_clustered, time_clust = clustering_func(get_log_vector(vector, duplicate), k, start_time, groups_col)
    evaluate_and_save_res(log, df_clustered, k, filenames, clustering_info, time_clust, start_time, index_col, metric_col, groups_col, silhouette_col)


def prepare(matrix_filename, duplicate, k, clustering_func_str, run, metadata_filename, index_col):
    #loads the log vector (matrix), combines the parameters and creates the filenames
    log_vector = read_matrix(matrices_path + matrices[matrix_filename], index_col=index_col)
    matrix_str_id = matrix_filename.replace('.csv','').replace('.npz','')
    dimensions = log_vector[0].shape[1]
    clustering_info = ','.join(matrix_str_id.split('_'))+','+str(duplicate)+','+str(dimensions)+','+str(k)+','+str(clustering_func_str)+','+str(run)
    filenames = get_results_filenames(matrix_filename, duplicate, k, clustering_func_str, run, metadata_filename)
    return log_vector, clustering_info, filenames


def read_matrix(filename,index_col=None):
    if '.csv' in filename:
        return pd.read_csv(filename, index_col=index_col), None, None
    # it is a crs matrix, so we will also need the columns and index
    cols_f = open('%s-cols.json'%filename.replace('.npz',''),'r')
    index_f = open('%s-rows.json'%filename.replace('.npz',''),'r')
    return sparse.load_npz(filename), json.load(cols_f), json.load(index_f)


def get_results_filenames(matrix, duplicate, k, method, run, metadata_filename):
    filenames = {}
    filenames['clustered_df']='kmeans_results/clustered_dfs/%s_duplicate%s_%s_k%s_run%s' % (matrix, str(duplicate), str(method), str(k), str(run))
    filenames['groups_data']='kmeans_results/groups_data/%s_duplicate%s_k%s_%s_run%s_group' % (matrix, str(duplicate), str(method), str(k), str(run))
    filenames['sublogs']='kmeans_results/sublogs/%s_duplicate%s_k%s_%s_run%s_group' % (matrix, str(duplicate), str(k), str(method), str(run))
    filenames['metadata'] = metadata_filename

    create_filenames_dirs(filenames)

    return filenames


def create_filenames_dirs(filenames):
    for filename in filenames:
        path = '/'.join(filenames[filename].split('/')[:-1])
        if not os.path.exists(path):
            os.makedirs(path)


def get_log_vector(vector, duplicate):
    log_vec = vector[0]
    if not isinstance(vector[0], pd.DataFrame): #it is a csr_matrix!
        log_vec = csr2df(vector)
    if not duplicate:    
        log_vec = log_vec.drop_duplicates()
    return log_vec


def csr2df(df):
    cols = df[1]
    index = df[2]
    df = pd.DataFrame(df[0].toarray())
    df.columns = cols
    df.index = index
    return df


def kmeans(log_vec, k, start_time, groups_col):
    res = KMeans(n_clusters=k).fit(log_vec)
    time_clust  = time.time() - start_time
            
    df_clustered = log_vec.copy()
    df_clustered[groups_col] = res.labels_

    return df_clustered, time_clust


def evaluate_and_save_res(log, df_clustered, k, filenames, clustering_info, time_clust, start_time, index_col, metric_col, groups_col, silhouette_col):
    if not isinstance(df_clustered, pd.DataFrame): #the clustering method couldn't find the amount of clusters that were asked
        save_metadata_incidents(filenames['metadata'], clustering_info, None, None, [], [], [], [], [])
        return 

    df_clustered, silhouette_avg, sample_silhouette_values, groups_silhouette_mean, groups_silhouette_std = get_silhouettes(df_clustered, k)
    groups_counts, durations_means, durations_stds = get_groups_info(k, df_clustered.reset_index(), log.reset_index(),
                                                                     index_col, groups_col, metric_col, 
                                                                     path_groups_data=filenames['groups_data'], path_sublogs=filenames['sublogs'])

    save_clustered_df(df_clustered[[groups_col, silhouette_col]], filenames['clustered_df'])
    time_total = time.time() - start_time
    save_metadata_incidents(filenames['metadata'], clustering_info, time_clust, time_total, groups_counts, 
                            groups_silhouette_mean, groups_silhouette_std, durations_means, durations_stds)


def get_silhouettes(df_clustered, k, groups_col='group', silhouette_col='silhouette'):
    silhouette_avg, sample_silhouette_values = calc_silhouette(df_clustered.drop(columns=groups_col).values, df_clustered[groups_col])
    df_clustered[silhouette_col] = sample_silhouette_values
    groups_silhouette_mean, groups_silhouette_std = calc_silhouette_per_group(df_clustered[groups_col], sample_silhouette_values)
    return df_clustered, silhouette_avg, sample_silhouette_values, groups_silhouette_mean, groups_silhouette_std


def get_groups_info(k, clustered_df, original_df, index_col, groups_col, measure_col, 
                     path_groups_data=None, path_sublogs=None):
    def save_group_data(group_data):
        if path_groups_data:
            group_data[[groups_col]].to_csv(path_groups_data+str(i)+'.csv')

    def get_sublog(group_data):
        group_indexes = list(group_data[index_col].unique())
        return original_df.loc[original_df[index_col].isin(group_indexes)]

    def save_sublog(group_elem_original_data):
        if path_sublogs:
            group_elem_original_data.to_csv(path_sublogs+str(i)+'.csv')

    qttd_elems = {}
    measure_col_means = {}
    measure_col_stds = {}
    for i in range(0,k):
        group_data = clustered_df[clustered_df[groups_col]==i]
        save_group_data(group_data)

        group_elem_original_data = get_sublog(group_data)
        save_sublog(group_elem_original_data)

        qttd_elems[i] = len(group_elem_original_data)
        measure_col_means[i] = group_elem_original_data[measure_col].mean()
        measure_col_stds[i] = group_elem_original_data[measure_col].std()
    return qttd_elems, measure_col_means, measure_col_stds


###################################### MAIN ###############################################

warnings.filterwarnings('ignore')

log1 = pd.read_csv("log/incident_evt_log-processed1-withdurations.csv")


matrices_path = 'matrices/'
matrices = get_matrices_filenames(matrices_path)
print('Vectors:', list(matrices.keys()))


ks = [3, 5, 10]
print('Ks:',ks)
duplicates = [True]
clustering_funcs = {'kmeans': kmeans}
runs = 10
print('#Runs/comb:', runs)
index_col = 'number'
measure_col = 'duration'

header = 'activity_represent,feature_selection,counting,duplicate,dimensions,k,method,run,time_clust,time_total,'
header+= 'group,elem_qtt,silhouette_mean,silhouette_std,duration_mean,duration_std'
metadata_filename = create_metadata_file('kmeans_results/metadata', header)

pbar = tqdm(total=len(matrices)*len(ks)*len(duplicates)*len(clustering_funcs)*runs)


for matrix_filename in matrices:
    for duplicate in duplicates:
        for k in ks:
            for clustering_func in clustering_funcs:
                for run in range(1,runs+1):
                    cluster_and_save(log1, matrix_filename, duplicate, k, clustering_funcs[clustering_func], clustering_func, 
                                     run, metadata_filename, index_col, measure_col)
                    pbar.update(1)
