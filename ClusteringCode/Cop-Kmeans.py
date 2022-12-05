#### Importings
import os
import time
import json
import warnings
import pandas as pd
import scripts.cop_kmeans as Cop

from tqdm import tqdm
from scripts.clustering_results import (create_metadata_file, calc_silhouette, calc_silhouette_per_group,
                                save_clustered_df, save_metadata_incidents)

#### Functions

def get_matrices_filenames(path):
    res = {}
    filenames = os.listdir(path)
    for filename in filenames:
        if '.csv' in filename and '._' not in filename:
            res[filename.replace('.csv','')] = filename
    return res


def cluster_and_save(log, matrix_filename, k, run, constraints_type, constraints_unique_perc, constraints_rep_perc, constraints, metadata_filename, index_col, metric_col, 
                     groups_col='group', silhouette_col = 'silhouette'):

    vector, clustering_info, filenames = prepare(matrix_filename, k, run, constraints_type, constraints_unique_perc, constraints_rep_perc, metadata_filename, index_col)
    ml, cl = constraints
    start_time = time.time()
    df_clustered, time_clust = cop(vector, k, start_time, groups_col, ml=ml, cl=cl)
    evaluate_and_save_res(log, df_clustered, k, filenames, clustering_info, time_clust, start_time, index_col, metric_col, groups_col, silhouette_col)


def prepare(matrix_filename, k, run, constraints_type, constraints_unique_perc, constraints_rep_perc, metadata_filename, index_col):
    #loads the log vector (matrix), combines the parameters and creates the filenames
    log_vector = read_matrix(matrices_path + matrices[matrix_filename], index_col=index_col)
    matrix_str_id = matrix_filename.replace('.csv','')
    dimensions = log_vector.shape[1]
    clustering_info = ','.join(matrix_str_id.split('_'))+','+str(dimensions)+','+str(k)+','+str(run)+','+constraints_type+','+str(constraints_unique_perc)+','+str(constraints_rep_perc)
    filenames = get_results_filenames(matrix_filename, k, run, constraints_type, constraints_unique_perc, constraints_rep_perc, metadata_filename)
    return log_vector, clustering_info, filenames


def read_matrix(filename, index_col=None):
    if '.csv' in filename:
        return pd.read_csv(filename, index_col=index_col)


def get_results_filenames(matrix, k, run, constraints_type, constraints_unique_perc, constraints_rep_count, metadata_filename):
    filenames = {}
    filenames['clustered_df']='cop_results/clustered_dfs/%s_k%s_run%s_%s_%sunique_%srep' % (matrix, str(k), str(run), constraints_type,str(constraints_unique_perc),str(constraints_rep_count))
    #filenames['groups_data']='cop_results/groups_data/%s_k%s_run%s_%s_%sunique_%srep-group_' % (matrix, str(k), str(run), constraints_type, str(constraints_unique_perc), str(constraints_rep_count))
    #filenames['sublogs']='cop_results/sublogs/%s_k%s_run%s_%s_%sunique_%srep-group_' % (matrix, str(k), str(run), constraints_type,str(constraints_unique_perc),str(constraints_rep_count))
    filenames['metadata'] = metadata_filename

    create_filenames_dirs(filenames)

    return filenames


def create_filenames_dirs(filenames):
    for filename in filenames:
        path = '/'.join(filenames[filename].split('/')[:-1])
        if not os.path.exists(path):
            os.makedirs(path)


def csr2df(df):
    cols = df[1]
    index = df[2]
    df = pd.DataFrame(df[0].toarray())
    df.columns = cols
    df.index = index
    return df


def cop(log_vec, k, start_time, groups_col, ml=None, cl=None):
    res, _, _, _ = Cop.cop_kmeans(dataset=log_vec.values, k=k, ml=ml, cl=cl)
    time_clust = time.time() - start_time
    
    df_clustered = log_vec.copy()
    df_clustered[groups_col] = res

    return df_clustered, time_clust


def evaluate_and_save_res(log, df_clustered, k, filenames, clustering_info, time_clust, start_time, index_col, metric_col, groups_col, silhouette_col):

    df_clustered, silhouette_avg, sample_silhouette_values, groups_silhouette_mean, groups_silhouette_std = get_silhouettes(df_clustered, k)
    groups_counts, durations_means, durations_stds = get_groups_info(k, df_clustered.reset_index(), log.reset_index(),
                                                                     index_col, groups_col, metric_col, 
                                                                     #path_groups_data=filenames['groups_data'], path_sublogs=filenames['sublogs']
                                                                     )

    save_clustered_df(df_clustered[[groups_col,silhouette_col]], filenames['clustered_df'])
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


def get_constraints(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
        data = {int(key): set(data[key]) for key in data}
        return data


def get_constraints_filenames(path):
    res = {}
    filenames = os.listdir(path)
    for filename in filenames:
        if '.json' in filename:
            res[filename.replace('.json','')] = (path+filename)
    return res


#### Main

warnings.filterwarnings('ignore')

# Get original data
log1 = pd.read_csv("log/incident_evt_log-processed1-withdurations.csv")
# Get matrices
matrices_path = 'matrices/'
matrices = get_matrices_filenames(matrices_path)
matrices = {key: matrices[key] for key in matrices if 'individual_specialist_tfidf' in key}
print('Vectors:', list(matrices.keys()))
# Get constraints
constraints_path = 'constraints/'
if not os.path.exists(constraints_path):
    os.makedirs(constraints_path)
all_constraints = get_constraints_filenames(constraints_path)
print('Constraints (first3):', list(all_constraints.keys())[:3])

# Cluster data
ks = [5]
print('Ks:',ks)
constraints_unique_percs = [1.0]#[0.10, 0.25, 0.50, 0.75, 1.0]
constraints_rep_percs = [1.0]#[0.10, 0.25, 0.50, 0.75, 1.0]
print('Constraints %:',constraints_unique_percs, constraints_rep_percs)
constraints_types = ['awaitingsml', 'easycategs', 'priority12x3x4']
print('Constraints types:', constraints_types)
runs = 10
print('#Runs per combination:', runs)

index_col = 'number'
measure_col = 'duration'

header = 'activity_represent,feature_selection,counting,dimensions,k,run,'
header += 'constraints_type,constraints_unique_perc,constraints_rep,time_clust,time_total,'
header += 'group,elem_qtt,silhouette_mean,silhouette_std,duration_mean,duration_std'
metadata_filename = create_metadata_file('cop_results/metadata', header)


pbar = tqdm(total=len(matrices)*len(ks)*runs*len(constraints_types)*
                     len(constraints_unique_percs)*len(constraints_rep_percs))

    
for matrix_filename in matrices:
    for k in ks:
        for constraints_type in constraints_types:
            for constraints_unique_perc in constraints_unique_percs:
                for constraints_rep_perc in constraints_rep_percs:
                    
                    aux = '%s-%s_%sunique_%srep-' %(matrix_filename, constraints_type, 
                                                        str(constraints_unique_perc), str(constraints_rep_perc))
                    
                    constraints = (get_constraints(all_constraints[aux + 'ml']), 
                                      get_constraints(all_constraints[aux + 'cl']))
                    for run in range(1,runs+1):
                        cluster_and_save(log1, matrix_filename, k, run, constraints_type, constraints_unique_perc, constraints_rep_perc, constraints, 
                                          metadata_filename, 'number', 'duration')
                        pbar.update(1)