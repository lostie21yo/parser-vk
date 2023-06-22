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
# location_key_words = "Татарстан|РТ|Республика татарстан|Казань|Челны|Сабинский|Тюлячи"
real_posts_key_words = ["Рак", "Страшный диагноз", "Метастаз", "Онкология", "Онколог", "Онкобольница", 
                        "Онкоцентр", "Онкодиспансер", "Хоспис", "Дексаметазон", "Химиотерапия", 
                        "Предраковое", "Лучевая терапия", "Злокачественное", "Новообразование", 
                        "Опухоль", "Низкие лейкоциты", "Онкопатолог", "Онкодиагностика", "Онкопрофилактика", 
                        "Профилактика онкологии", "Онкомаркер", "онкодиагноз","Эшэке чир", "яман чир", 
                        "Куркыныч диагноз", "Онкологич үзәге", "Дексаметазоналар", "Башланып килэ торган чир", 
                        "Яман шеш", "Начар шеш", "Лучивая терапиясе", "Түбән лейкоцитлар", 
                        "яки онкологияне профилактикалау"]
posts_key_words = [r" Рак ", r"Страшный диагноз", r"Метастаз", r"Онкология", r"Онколог", r"Онкобольниц", 
                   r"Онкоцентр", r"Онкодиспансер", r"Хоспис", r"Дексаметазон", r"химиотерап", 
                   r"Предраков", r"Лучевая терапия", r"Злокачественн", r"Новообразовани", 
                   r"Опухол", r"Низкие лейкоциты", r"онкопатолог", r"онкодиагностик", r"онкопрофилактик", 
                   r"профилактика онкологии", r"онкомаркер", r"онкодиагноз", r"Эшэке чир", r"яман чир", 
                   r"Куркыныч диагноз", r"Онкологич үзәге", r"Дексаметазоналар", r"Башланып килэ торган чир", 
                   r"Яман шеш", r"Начар шеш", r"Лучивая терапиясе", r"Түбән лейкоцитлар", 
                   r"яки онкологияне профилактикалау"]
# posts_key_words = r" рак |Страшный диагноз|Метастаз|Онколог|Онкобольниц|Онкоцентр|Онкодиспансер|Хоспис|Дексаметазон|химиотерапи|Предраков|Лучевая терапия|Злокачественн|Новообразовани|Опухоль|Лучевая терапия|Низкие лейкоциты|онкопатологи|онкодиагностик|онкопрофилактик|профилактика онколог|онкомаркер"

user_token = "vk1.a.7csDDUqi8Uy77K6uL_b5fVwKxrxpay5zWpKjhQp_Xa9clsHCWc4msErqcfPhy1P0CRUKcGAVGbSAg8E-2vyHcUayWb2eTodGEPJNzd-Hoaavs0QxIflmP-2pf4426_QwDxSmVCFwc7bMRuIFhoPiwqTn2UuzvsjU5nqZD9kqpp4hAc6HJUZqcJhUpJQvHjOiT7jE_Ts48dWLEvwIMnSQog"

api_version = 5.131


# создание файла csv
file_dir = os.path.split(__file__)[0].replace('\\', '/')
try:
    df = pd.read_csv(f"{file_dir}/post_statistic.csv", delimiter='\t', encoding='utf-16') # old
except:
    df = pd.DataFrame(columns=('Ключ', 'Локация (РТ)', 'Домен', 'Ссылка', 'Пост (текст)', 'Дата', 'Лайки', 'Комментарии', 'Репосты', 'Просмотры')) # new



# через api vk вызываем список групп по ключевому слову
def get_list_of_groups(q, os):
    response = requests.get('https://api.vk.com/method/search.getHints',
                            params={'access_token': user_token,
                                    'v': api_version,
                                    'q': q,
                                    'limit': 100,
                                    'offset': os,
                                    'filters': "groups,publics,events"})

    items = response.json()['response']['items']
    for i in range(len(items)):
        if ((items[i]["type"] in ("group", "public", "event")) 
            and (items[i]["group"]["screen_name"] not in list_of_groups) 
            and ("auto" not in items[i]["group"]["screen_name"])
            and ("avto" not in items[i]["group"]["screen_name"])
            and ("baraholka" not in items[i]["group"]["screen_name"])
            and ("arend" not in items[i]["group"]["screen_name"])):
            list_of_groups.append(items[i]["group"]["screen_name"])
            list_of_groups_loc.append(q)

    
    return response.json()['response']['count']

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


# получение списка групп
list_of_groups = []
list_of_groups_loc = []
for loc_key in location_key_words:
    group_count = 0
    for group_offset in range(0, 1000, 100):
        try:
            # print("group_offset: ", group_offset)
            group_count += get_list_of_groups(loc_key, group_offset)
        except:
            break
        time.sleep(1)
    print(f'loc_key: {loc_key}, group_count: {group_count}')
print("unique groups count: ", len(list_of_groups))
print("list_of_groups_loc:", len(list_of_groups_loc))

# настройка списка доменов
# Сделано 0-415, 500-600, 1000-1175 (525/1175)
lower = 415
upper = 1010
list_of_groups = list_of_groups[lower:upper]
list_of_groups_loc = list_of_groups_loc[lower:upper]

# получение списка постов каждой группы и сохранение статистики
for k in range(len(list_of_groups)):
    district = list_of_groups_loc[k]
    print(f'Домен {k+1}/{len(list_of_groups)} - {list_of_groups[k]}')
    
    for post_offset in trange(0, 1000, 100):
        time.sleep(1)
        try:
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
                        df.loc[len(df.index)] = [key_word, district, list_of_groups[k], link, text, date, likes, comments, reposts, views]
                        # сохранение
                        df = df.drop_duplicates(subset=['Пост (текст)'])
                        df.to_csv(f'{file_dir}/post_statistic.csv', sep='\t', index= False, encoding="utf-16")
        except:
            print(f"\nупс, маловато постов похоже")
            # сохранение в файл
            df = df.drop_duplicates(subset=['Пост (текст)'])
            df.to_csv(f'{file_dir}/post_statistic.csv', sep='\t', index= False, encoding="utf-16")
            break

        