FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

RUN pip install -U pip
RUN pip install pipenv 

WORKDIR /app
COPY [ "Pipfile", "Pipfile.lock", "./" ]

RUN mkdir output
RUN pipenv install --system --deploy

COPY [ "starter.py", "./" ]

ENTRYPOINT [ "/bin/bash"]
# ENTRYPOINT [ "python", "starter.py", "fhv", "2021", "4" ]