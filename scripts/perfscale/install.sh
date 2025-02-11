#!/bin/bash
set -a
_user=$(oc whoami)
usage() { echo "Usage: $0 [-p <string> (airflow password)]" 1>&2; exit 1; }
GIT_ROOT=$(git rev-parse --show-toplevel)
source $GIT_ROOT/scripts/common.sh
source $GIT_ROOT/scripts/perfscale/check_user_and_repo.sh
_airflow_namespace=airflow

while getopts p: flag
do
    case "${flag}" in
        p) password=${OPTARG};;
        *) usage;;
    esac
done

if [[ -z "$password" ]]; then 
    usage
fi



install_argo(){    
    kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
}

pre_install(){
    kubectl create namespace argocd || true
    kubectl create namespace fluentd || true
    kubectl create namespace airflow || true
    kubectl create namespace openshift-logging || true
    kubectl create namespace openshift-operators-redhat || true
    kubectl create namespace elastic-system || true
    kubectl create namespace perf-results || true
    kubectl apply -f $GIT_ROOT/scripts/perfscale/raw_manifests/
}

add_privileged_service_accounts(){
    oc adm policy add-scc-to-group privileged system:authenticated
}


install_perfscale(){
    cd $GIT_ROOT/charts/perfscale
    envsubst < $GIT_ROOT/scripts/perfscale/values/install.yaml
    envsubst < $GIT_ROOT/scripts/perfscale/values/install.yaml | helm upgrade perfscale . --install --force --namespace argocd -f -

}

wait_for_apps_to_be_healthy(){
    argocd login $(oc get route/argocd -o jsonpath='{.spec.host}' -n argocd) --username admin --password $(kubectl get secret/argocd-initial-admin-secret -n argocd -o jsonpath='{.data.password}' | base64 --decode) --insecure
    argocd app wait -l app.kubernetes.io/managed-by=Helm
}

post_install(){
    _results_elastic_password=$(kubectl get secret/perf-results-es-elastic-user -o jsonpath='{.data.elastic}' -n perf-results | base64 --decode)
    cd $GIT_ROOT/charts/perfscale
    envsubst < $GIT_ROOT/scripts/perfscale/values/update.yaml | helm upgrade perfscale . --install --force --namespace argocd -f -
    oc -n openshift-logging delete pod -l component=fluentd
}


echo "Installing Dependencies"
install_argo_cli > /dev/null 2>&1
install_helm > /dev/null 2>&1
echo "Creating Namespaces and other unconfigurable manifests if they don't exist..."
pre_install > /dev/null 2>&1
echo "Creating services accounts"
add_privileged_service_accounts > /dev/null 2>&1
echo "Installing Argo"
install_argo > /dev/null
sleep 60
echo "Installing PerfScale Platform"
install_perfscale
echo "PerfScale Platform Creating, waiting for Applications to become healthy"
wait_for_apps_to_be_healthy
post_install
output_info