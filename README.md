# POC Airflow
## Informations
This project is built on Docker, so you will need to install these first
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)

## Installation
Please follow the steps in order to install the image:
#### Build the Image 
This is done from inside the project
```    
airflow_poc $ docker build -t airflow_poc/docker-airflow .
```

#### Run the containers
```
airflow_poc $ docker-compose up
```

### SSH & Create Admin User
* SSH to the Webserver Container (open another tab) & Create User
```
$ docker exec -it airflow_poc_webserver_1 bash
$ python dags/library_files/create_user.py
```

### UI Links
- Airflow Webserver: [localhost:8080](http://localhost:8080/)
- Flower: [localhost:5555](http://localhost:5555/)

