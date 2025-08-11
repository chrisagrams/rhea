# Deploying Rhea

## Docker (Local)

```
docker compose -f deploy/docker-compose.yaml up -d
```

With GPU support (for embedding model):

```
docker compose -f deploy/docker-compose.gpu.yaml -d
```
