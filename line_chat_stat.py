# -*- coding: utf-8 -*-
import re
import time
import datetime
import os.path
import codecs

source = codecs.open("./cto_line_1216.txt", 'r');
target = codecs.open("./target", 'w', 'utf-8');

lineCountPerDate = {};
for line in source.readlines():

    # 2016. 01. 01. (금)
    if re.match('\d{4}[./] \d{2}[./] \d{2}', line):
        dt = datetime.datetime.strptime(line[0:13], '%Y. %m. %d.');
        lineCountPerDate[dt.strftime("%Y-%m-%d")] = 0;

    # 13:20
    elif re.match('\d{2}:\d{2}', line):
        dateYmd = sorted(lineCountPerDate.keys())[-1];
        lineCountPerDate[dateYmd] += 1;

for key, value in lineCountPerDate:
    target.write('{} {}\n'.format(key, value));

# stickerCountPerPerson = {};
# for line in source.readlines():

#     # # 2016. 01. 01. (금)
#     # if re.match('\d{4}[./] \d{2}[./] \d{2}', line):
#     #     dt = datetime.datetime.strptime(line[0:13], '%Y. %m. %d.');
#     #     curDate = dt.strftime("%Y-%m-%d");
    
#     # 13:20
#     if re.match('\d{2}:\d{2}', line):
#         name = re.search(r"\s\w*", line).group()[1:];
#         if name not in stickerCountPerPerson:
#             stickerCountPerPerson[name] = 0;

#         if "[스티커]" in line:
#             stickerCountPerPerson[name] += 1;

# for key, value in sorted(stickerCountPerPerson.items()):
#     target.write('{} {}\n'.format(key, stickerCountPerPerson[key]));

