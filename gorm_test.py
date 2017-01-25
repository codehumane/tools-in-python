import mysql.connector
import gorm

# myconn = mysql.connector.connect(host="호스트명", user="계정", database="DB명", port="포트", buffered=True);
# mycursor = myconn.cursor();
# 
# # TEST......
# mycursor.execute("SELECT uid, gid, groupname FROM gadr_group");
# gadr_group_list = mycursor.fetchall();
# print(gadr_group_list);

# # 연결
gpg = gorm.pg({
    "host": "호스트", 
    "dbname": "DB명", 
    "user": "계정",
    "password": "비밀번호"
});


# # 삽입
# BOOK_ID = gpg.insert('go_contact_books', {
#     'created_at': 'now()',
#     'updated_at': 'now()',
#     'contact_count': 0,
#     'company_id': 1
# });
# print(BOOK_ID);

# GROUP_ID = gpg.insert('go_contact_groups', {
#     'book_id': BOOK_ID,
#     'name': '안녕하세요',
#     'public_flag': 't',
#     'seq': 0,
#     'type': 'NORMAL'
# });
# print(GROUP_ID);



# 단일 조회
one = gpg.selectOne('go_users', ['id'], {
    'statement': "login_id=%s",
    'params': ["chogh1211"]
});
print(one);
print("ok.....");

gpg.update('go_users', {'name' : '조거니'}, {
    'statement' : 'login_id=%s',
    'params' : ['chogh1211']
});

# two = gpg.selectOne('go_contact_groups', ['id'], {
#     'statement': "name=%s and public_flag=%s",
#     'params': ['안녕하세요', 't']
# });
# print(two);


# # 다수 조회
# found = gpg.selectAll('go_contact_books', ['created_at', 'updated_at', 'contact_count', 'company_id'], {});
# print(found);




# # count
# count = gpg.count('go_contact_books', {});
# print(count);






# --------------------------------- #
# mssql
# --------------------------------- #


# gmssql = gorm.mssql({
#     "host": "호스트명", 
#     "dbname": "DB명", 
#     "user": "계정",
#     "password": "비밀번호"
# });
# 
# depts = gmssql.selectAll('userinfo', ['UserSQ'], {});
# print(len(depts));

