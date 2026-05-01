FROM python:3.10-slim

# set working folder inside container
WORKDIR /app

# copy all project files into container
COPY . .

# install python libraries
RUN pip install --no-cache-dir -r requirements.txt

# run your pipeline
CMD ["python", "-m", "pipeline.main"]