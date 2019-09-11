# RECEIVE MESSAGE RABBITMQ - PYTHON

## Create virtualenv
```bash
virtualenv venv
```
## Activate virtualenv
```bash
.\venv\Scripts\activate.ps1
```
or
```bash
.\venv\Scripts\activate.bat
```

## Installation
```bash
pip install -r requirements.txt
```
## Change settings

>config.json

## Test
```bash
py .\app\app.py
```

## Build image with Docker
```bash
docker build -t nameimage .
```

## Run container
```bash
docker run -it --rm nameimage
```

