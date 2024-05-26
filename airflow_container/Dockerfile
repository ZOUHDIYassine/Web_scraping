FROM apache/airflow:2.8.1

USER root

# Install dependencies for Chrome and WebDriver
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    xvfb \
    gnupg

# Install Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable

# Install Chrome WebDriver
RUN wget -O /tmp/chrome-linux64.zip https://storage.googleapis.com/chrome-for-testing-public/125.0.6422.76/linux64/chromedriver-linux64.zip && \
    unzip /tmp/chrome-linux64.zip -d /usr/local/bin/


# Switch back to the airflow user
USER airflow

# Copy custom Python modules to the container
#COPY /dags/time_parser /opt/airflow/custom_modules/time_parser
#COPY /dags/article_scrap /opt/airflow/custom_modules/article_scrap
#COPY /dags/time_encoder /opt/airflow/custom_modules/time_encoder

# Copy requirements.txt to the container and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow

