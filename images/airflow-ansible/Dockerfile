FROM apache/airflow:latest
USER root

RUN apt update -y
RUN apt install jq git build-essential uuid-runtime gettext-base xz-utils sshpass -y


RUN curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl"
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl
RUN curl -sS https://mirror.openshift.com/pub/openshift-v4/clients/ocp/latest/openshift-client-linux.tar.gz | tar xz oc --directory /usr/local/bin/
RUN ln -s /bin/bash /usr/bin/bash

USER airflow
RUN yes | pip install ansible netaddr || exit 0