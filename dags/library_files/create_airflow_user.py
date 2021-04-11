import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser


def create_user(username: str, password: str, email: str,
                is_superuser: bool = True):
    """
    Method to create Airflow user

    :param username: (str) - Airflow User name
    :param password: (str) - Airflow Password
    :param email: (str) - Email of the user
    :param is_superuser: (str) - Is super user - True/False

    Note:
        Modified date: 10-04-2021
        Author: TB
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

    Note:
        Modified date: 10-04-2021
        Author: TB
    """
    username = input("Enter username :")
    password = input("Enter password :")
    email = input("Enter email :")
    create_user(username, password, email)


init_user()
