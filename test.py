from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
User = PasswordUser(models.User())

# User Info
user.username = 'defect-rest'
user.email = 'jaducku@kakao.com'
user.password = '1234'

session = settings.Session()
user_exits = session.query(models.User.id).filter_by(username=user.username).scalar() is not None
if not user_exits:
    session.add(user)
    session.commit()
session.close()