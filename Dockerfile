FROM mageai/mageai:latest

ARG USER_CODE_PATH=/home/src/dew1

# Install required packages
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-x64_bin.tar.gz && \
    tar -xzf openjdk-17.0.2_linux-x64_bin.tar.gz && \
    rm -rf /opt/jdk-17.0.2 && \
    mv jdk-17.0.2 /opt/ && \
    rm openjdk-17.0.2_linux-x64_bin.tar.gz && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME and PATH
ENV JAVA_HOME=/opt/jdk-17.0.2
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy the requirements file
COPY requirements.txt ${USER_CODE_PATH}/requirements.txt 

# Install Python dependencies
RUN pip3 install -r ${USER_CODE_PATH}/requirements.txt

# Install additional dependencies for PySpark if needed
RUN pip3 install pyspark

# Set the working directory
WORKDIR ${USER_CODE_PATH}

# Copy the rest of the application code
COPY . ${USER_CODE_PATH}
