import pandas as pd
import os
from collections import Counter

file_dir = os.path.split(__file__)[0].replace('\\', '/')
df = pd.read_csv(f"{file_dir}/post_statistic_p1.csv", delimiter='\t', encoding='utf-16') # old


# подсчет кол-ва вхождений каждого элемента в список
def duplicate_counting(df, limit):
    array = []
    for i in range(len(df.values)):
        array.append(f'{df.values[i][0]} {df.values[i][1]} {df.values[i][2]}')

    c = Counter(array).most_common(limit)
    for elem in c:
        print(elem)


# df = df.drop_duplicates(subset=['Домен'], keep='first')
# df = df.sort_values(by=['Домен'])

# df.to_csv(f'{file_dir}/post_statistic.csv', sep='\t', index= False, encoding="utf-16")

with open(f"{file_dir}/for20more.txt", mode='w') as file:
    file.write(f'"{list_of_groups[k]}", ')