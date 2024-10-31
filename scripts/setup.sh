#!/bin/bash

wait_for_pod_ready() {
    local app_label=$1
    local timeout=120  # 120 seconds timeout
    local interval=10  # Check every 10 seconds
    local start_time=$(date +%s)

    # Get the pod name based on the app label
    pod_name=$(kubectl get pods -l $app_label -o jsonpath='{.items[0].metadata.name}')
    if [ -z "$pod_name" ]; then
        echo "No pod found with label: $app_label"
        return 1
    fi

    # Loop to check pod readiness
    echo "Waiting for pod '$pod_name' to be ready..."
    while true; do
        # Check if the pod is ready
        ready_status=$(kubectl get pod "$pod_name" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
        if [ "$ready_status" == "True" ]; then
            echo "Pod '$pod_name' is ready."
            return 0
        fi

        # Check if the timeout has been reached
        current_time=$(date +%s)
        elapsed_time=$((current_time - start_time))
        if [ "$elapsed_time" -ge "$timeout" ]; then
            echo "Timeout reached. Pod '$pod_name' is not ready."
            return 1
        fi
        echo "Pod '$pod_name' is not ready yet. Checking again in $interval seconds..."
        sleep $interval
    done
}

launch_spark_application() {
  local SPARK_APP_NAME=$1    # SparkApplication name (metadata.name)
  local MANIFEST_FILE=$2     # Path to SparkApplication manifest file
  local TIMEOUT=$3           # Timeout in seconds

  # Apply the SparkApplication manifest to start the job
  echo "Launching SparkApplication $SPARK_APP_NAME using manifest $MANIFEST_FILE..."
  kubectl apply -f "$MANIFEST_FILE"

  # Monitor the driver pod status
  echo "Monitoring SparkApplication status..."
  sleep 3
  if kubectl wait pods -l "job-name=$SPARK_APP_NAME" \
    --for=jsonpath='{.status.phase}'=Succeeded --timeout="${TIMEOUT}s"; then
    echo "SparkApplication completed."

    # Check the status of the SparkApplication custom resource
    local phase
    phase=$(kubectl get sparkapplication "$SPARK_APP_NAME" -o jsonpath='{.status.phase}')
    if [[ "$phase" == "Succeeded" ]]; then
      echo "SparkApplication completed successfully."
      kubectl delete -f "$MANIFEST_FILE"
      return 0
    elif [[ "$phase" == "Failed" ]]; then
      echo "SparkApplication failed."
      kubectl delete -f "$MANIFEST_FILE"
      return 1
    else
      echo "SparkApplication in non-terminal phase: $phase."
      kubectl delete -f "$MANIFEST_FILE"
      return 1
    fi

  else
    # Handle timeout or failure
    echo "SparkApplication failed or timed out."
    echo "Removing Spark custom resource..."
    kubectl delete -f "$MANIFEST_FILE"
    return 1
  fi
}

# Create kind cluster
kind create cluster

# Update helm repositories
helm repo add stackable https://repo.stackable.tech/repository/helm-stable/
helm repo add minio https://operator.min.io/
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

# Deploy stackable operators
echo "Deploying stackable operators..."
helm install commons-operator stackable/commons-operator --version 24.7.0
helm install secret-operator stackable/secret-operator --version 24.7.0
helm install hive-operator stackable/hive-operator --version 24.7.0
helm install spark-k8s-operator stackable/spark-k8s-operator --version 24.7.0
helm install trino-operator stackable/trino-operator --version 24.7.0
helm install superset-operator stackable/superset-operator --version 24.7.0

# Deploy minio service
echo "Deploying minio..."
helm install minio \
--version 4.0.2 \
--set mode=standalone \
--set replicas=1 \
--set persistence.enabled=false \
--set buckets[0].name=lakehouse \
--set buckets[0].policy=public \
--set rootUser=admin \
--set rootPassword=adminadmin \
--set resources.requests.memory=1Gi \
--set service.type=NodePort,service.nodePort=null \
--set consoleService.type=NodePort,consoleService.nodePort=null \
--repo https://charts.min.io/ minio
echo "Waiting for minio pod..."
sleep 2
wait_for_pod_ready "app=minio"

# Deploy s3 connection
echo "Deploying s3 connection..."
kubectl apply -f s3-connection.yaml

# Deploy hive service
echo "Deploying hive..."
kubectl apply -f hive.yaml
echo "Waiting for hive pod..."
sleep 30
wait_for_pod_ready "app.kubernetes.io/name=hive"

# Deploy spark applications
launch_spark_application telemetry-generator telemetry-generator.yaml 240
launch_spark_application telemetry-statistics telemetry-statistics.yaml 240

# Deploy trino service
echo "Deploying trino..."
kubectl apply -f trino.yaml

# Deploy superset service
echo "Deploying superset..."
helm install --wait superset bitnami/postgresql \
    --set auth.username=superset \
    --set auth.password=superset \
    --set auth.database=superset
kubectl apply -f superset.yaml
sleep 20
kubectl rollout status --watch statefulset/superset-node-default --timeout 300s

# Setup superset service
echo "Configuring superset..."
kind load docker-image pulse-analytics:latest --name kind
kubectl apply -f superset-setup.yaml

# Port-forward to localhost
kubectl port-forward svc/superset-external 8088:8088
