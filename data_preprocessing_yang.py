import os
import sys
import csv
import pandas as pd
import time

def main():

    path = sys.argv[1]

    #Job events table: job id [2], user name [4] (poderia ser task events, mas é maior)
    #Task usage table: start time [0], end time [1], job id [2], task index [3]
    #Tem tb vários dados de CPU e Memória, quais considerar? mean cpu usage rate?

    #Dividir os usuários
    #Para cada usuário, somar os recursos usados em cada período de tempo
    #Converter esses recursos para número de instâncias t2.nano
    #Gerar uma tabela com a demanda de instâncias por usuário

    instance_capacity = 1 #atualizar dps
    global_start_time = 600000000 #600s
    global_end_time = 2506200000000 #2.506.200s

    job_users, users = get_job_users(path)
    for user in users:
        demand_table = create_demand_table(global_start_time, global_end_time, user)
        for filename in os.scandir(path + '/task_usage'):
            task_usage = pd.read_csv('processed_data/task_usage_2/part-00000-of-00500.csv')
            for i in range(len(task_usage)):
                task_usage_line = task_usage.iloc[i]
                task_user = job_users[task_usage_line[2]]
                if user == task_user:
                    start_time = task_usage_line[0]
                    end_time = task_usage_line[1]
                    usage = task_usage_line[5] #mean cpu usage rate
                    demand_table[int(start_time/10**6)-int(global_start_time/10**6) + 1] += usage/instance_capacity
                    demand_table[int(end_time/10**6)-int(global_start_time/10**6) + 2] -= usage/instance_capacity
            break #temporario

        for i in range(2, len(demand_table)):
            demand_table[i] += demand_table[i-1]
        
        output = open('/user_demands/' + user + '_demands.csv', 'w')
        writer = csv.writer(output)
        for line in demand_table:
            writer.writerow(line)
        output.close
        break #temporario
            
    # Os intervalos de tempo não são iguais (dificuldade: encontrar o grão (acho que é 10^6))
    # max: 2,5044 * 10^12 (2,5 trilhões de linhas - é demais)
    # considerando o grão como 10^6 (segundo): 2,5044 * 10^6 (2,5 milhões de linhas -> cada tabela já tem pelo menos 1 milhão de linhas)

#Returns a dictionary with the jobs and its users and a list of users
def get_job_users(path):
    job_users = {}
    users = set()
    ind = 0
    for filename in os.scandir(path + '/job_events'):
        job_events = pd.read_csv(filename)
        for i in range(len(job_events)):
            job_events_line = job_events.iloc[i]
            job_users.update({job_events_line[0]: job_events_line[1]})
            users.add(job_events_line[1])
        if ind%50 == 0:
            print(ind/500 * 100)
        ind += 1
    return (job_users, users)

def create_demand_table(start_time, end_time, user):
    table = [['time', user]]

    for t in range(int(start_time/10**6), int(end_time/10**6) + 1):
        line = [t, 0]
        table.append(line)

    return table

if __name__ == "__main__":
    main()