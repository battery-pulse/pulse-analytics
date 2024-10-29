.PHONY: docker-image
docker-image:
	docker build -t pulse-analytics:latest .