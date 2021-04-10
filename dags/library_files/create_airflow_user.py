import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser


def create_user(username, password, email, is_superuser=True):
    """
    Method to create Airflow user
    :param username: User name
    :param password: Password
    :param email: Email of the user
    :param is_superuser: Is super user - True/False
    """
    user = PasswordUser(models.User())
    user.username = username
    user.email = email
    user.password = password
    user.superuser = is_superuser
    session = settings.Session()
    session.add(user)
    session.commit()
    session.close()


def init_user():
    """
    Take input from user & create user
    :return:
    """
    username = input("Enter username :")
    password = input("Enter password :")
    email = input("Enter email :")
    create_user(username, password, email)


init_user()
