# Use an official Python runtime as an image
FROM python:3.7

# The EXPOSE instruction indicates the ports on which a container
# will listen for connections
# Since Flask apps listen to port 5000  by default, we expose it
EXPOSE 8080
USER root
# Sets the working directory for following COPY and CMD instructions
# Notice we haven’t created a directory by this name - this instruction
# creates a directory with this name if it doesn’t exist
WORKDIR /var/www/python

RUN apt-get update
RUN apt-get install gcc libssl-dev python3-dev unixodbc unixodbc-dev -y
# Install any needed packages specified in requirements.txt
COPY . /var/www/python
RUN ["chmod", "+x" , "/var/www/python/docker/odbc.sh"]
RUN ["chmod", "+x" , "/var/www/python/docker/selenium.sh"]
RUN ["chmod", "+x" , "/var/www/python/main.py"]
RUN /var/www/python/docker/odbc.sh
RUN /var/www/python/docker/selenium.sh
RUN pip install -r requirements_py38.txt
RUN pip install requests[security]
#CMD ["python", "main.py"]
