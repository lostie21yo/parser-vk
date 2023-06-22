# -*- coding: utf-8 -*-
import csv #для вывода информации в csv
from datetime import datetime #для перевода даты
import requests #для запросов
import time #для задержки между запросоми
import pandas as pd
import os
import re
from tqdm import trange


location_key_words = ["Татарстан", " РТ ", "Республика татарстан", "Казань", "Челны", "Сабинский", "Тюлячи", "Нижнекамск", "Арск", "Агрыз", "Елабуга", "Альметьевск"]
real_posts_key_words = ["Рак", "Страшный диагноз", "Метастаз", "Онкология", "Онколог", "Онкобольница", 
                        "Онкоцентр", "Онкодиспансер", "Хоспис", "Дексаметазон", "Химиотерапия", 
                        "Предраковое", "Лучевая терапия", "Злокачественное",  
                        "Опухоль", "Низкие лейкоциты", "Онкопатолог", "Онкодиагностика", "Онкопрофилактика", 
                        "Профилактика онкологии", "Онкомаркер", "онкодиагноз", "Новообразование", "Эшэке чир", "яман чир", 
                        "Куркыныч диагноз", "Онкологич үзәге", "Дексаметазоналар", "Башланып килэ торган чир", 
                        "Яман шеш", "Начар шеш", "Лучивая терапиясе", "Түбән лейкоцитлар", 
                        "яки онкологияне профилактикалау"]
posts_key_words = [r" Рак ", r"Страшный диагноз", r"Метастаз", r"Онкологи", r"Онколог", r"Онкобольниц", 
                   r"Онкоцентр", r"Онкодиспансер", r"Хоспис", r"Дексаметазон", r"химиотерап", 
                   r"Предраков", r"Лучевая терапия", r"Злокачественн", 
                   r"Опухол", r"Низкие лейкоциты", r"онкопатолог", r"онкодиагностик", r"онкопрофилактик", 
                   r"профилактика онкологии", r"онкомаркер", r"онкодиагноз", r"Новообразовани", r"Эшэке чир", r"яман чир", 
                   r"Куркыныч диагноз", r"Онкологич үзәге", r"Дексаметазоналар", r"Башланып килэ торган чир", 
                   r"Яман шеш", r"Начар шеш", r"Лучивая терапиясе", r"Түбән лейкоцитлар", 
                   r"яки онкологияне профилактикалау"]

user_token = "vk1.a.8R0RxjwcPqf717y5XUkKUSC0TKe6Ay8nMobNxDF6-0xRAxJTi9cSraLjI7G8zt6JMrqzFmJHtnjtQK_SMJ75FWmBeZoT3EeA4VIql-_b3f27S_Xi2GldetfQAhROELdV_Qoawj1g2mS-xNfQYG349PSMjECCa7eY2xzFgqIu8wSwNxHRiKt3n462bGjpUhYZTojg-93RTCgoD8YNAWDaDg"
api_version = 5.131


# чтение и создание файлов csv
file_dir = os.path.split(__file__)[0].replace('\\', '/')
df1 = pd.read_csv(f"{file_dir}/post_statistic.csv", delimiter='\t', encoding='utf-16') # old

try:
    df2 = pd.read_csv(f"{file_dir}/post_statistic_p2.csv", delimiter='\t', encoding='utf-16') # old
except:
    df2 = pd.DataFrame(columns=('Ключ', 'Локация (РТ)', 'Домен', 'Ссылка', 'Пост (текст)', 'Дата', 'Лайки', 'Комментарии', 'Репосты', 'Просмотры')) # new




# через api vk вызываем статистику постов определенного сообщества
def get_wall_posts_from_group(d, os):
    response = requests.get('https://api.vk.com/method/wall.get',
                            params={'access_token': user_token,
                                    'v': api_version,
                                    'domain': d,
                                    'count': 100,
                                    'offset': os,
                                    # 'filter': str('owner')
                                    })

    data = response.json()['response']['items']
    return data


# из готового списка
df1 = df1.drop_duplicates(subset=['Домен'], keep='first')
df1 = df1.sort_values(by=['Лайки'], ascending=False)
list_of_groups = list(df1['Домен'])
list_of_groups_loc = list(df1['Локация (РТ)'])

# for i in range(len(list_of_groups)):
#     print(f"{list_of_groups_loc[i]}              {list_of_groups[i]}")

# настройка списка доменов 
start_offset = 10000
stop_offset = 20000
lower = list_of_groups.index("perinatalka")
# upper = 195
list_of_groups = list_of_groups[lower:]
list_of_groups_loc = list_of_groups_loc[lower:]
print(list_of_groups)
"""Проверено постов: 
- первые 15 000 [0-5]
- первые 10 000 [6-END]
- с 10 000 до 15 000 [tat.gimnaziya1 - cadovnikagryz-1] tipi4nelabuga
- с 10 000 до 20 000 [0 - chelnychs, cadovnikagryz - ]

из 205
"""

# получение списка постов каждой группы и статистики
for k in range(len(list_of_groups)):
    # if list_of_groups[k] == 'vakansiialmetevsk':
    #     start_offset = 10000
    try:
        df2 = pd.read_csv(f"{file_dir}/post_statistic_p2.csv", delimiter='\t', encoding='utf-16') # old
    except:
        df2 = pd.DataFrame(columns=('Ключ', 'Локация (РТ)', 'Домен', 'Ссылка', 'Пост (текст)', 'Дата', 'Лайки', 'Комментарии', 'Репосты', 'Просмотры')) # new
    district = list_of_groups_loc[k]
    print(f'Домен {k+1}/{len(list_of_groups)} - {list_of_groups[k]}')
    counter = 0
    flag = True
    before_line_count = len(df2.index)
    for post_offset in trange(start_offset, stop_offset, 100):
        time.sleep(0.3)
        # try:
        post_package = get_wall_posts_from_group(list_of_groups[k], post_offset)
        for j in range(len(posts_key_words)):
            for i in range(len(post_package)):
                if len(re.findall(posts_key_words[j], post_package[i]["text"], re.IGNORECASE))>0:
                    # list_of_posts.append(post_package[j])
                    key_word = real_posts_key_words[j]
                    link = 'https://vk.com/wall'+str(post_package[i]["from_id"])+'_'+str(post_package[i]["id"])
                    text = post_package[i]['text'].replace('\n', ' ')
                    date = datetime.fromtimestamp(post_package[i]['date']).strftime("%d-%m-%Y") #"%Y-%m-%d %H:%M"
                    likes = post_package[i]['likes']['count']
                    comments = post_package[i]['comments']['count']
                    reposts = post_package[i]['reposts']['count']
                    views = post_package[i]['views']['count']
                    df2.loc[len(df2.index)] = [key_word, district, list_of_groups[k], link, text, date, likes, comments, reposts, views]
                    counter += 1
                    if counter >= 50 and flag:
                        flag = False
                        with open(f"{file_dir}/for20more.txt", mode='a') as file:
                            file.write(f'"{list_of_groups[k]}", ')
                            
                    # сохранение
                    # df2 = df2.drop_duplicates(subset=['Пост (текст)'], keep='last')
                    # df2.to_csv(f'{file_dir}/post_statistic_p2.csv', sep='\t', index= False, encoding="utf-16")
        # except:
        #     # print(f"\nупс, маловато постов похоже")
        #     break
    # сохранение в файл
    df2 = df2.drop_duplicates(subset=['Пост (текст)'], keep='last')
    print(f"Обнаружено {counter} строк (было {before_line_count}, стало {len(df2.index)}, без дупликатов добавлено {len(df2.index)-before_line_count})\n")
    df2.to_csv(f'{file_dir}/post_statistic_p2.csv', sep='\t', index= False, encoding="utf-16")

        