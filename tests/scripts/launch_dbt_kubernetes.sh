#!/bin/bash

kubectl apply -f dbt-job.yaml
kubectl wait --for=condition=complete --timeout=180s job/dbt-job
kubectl delete -f dbt-job.yaml
