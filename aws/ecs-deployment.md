# AWS ECS Deployment for Fraud Detection

## Prerequisites
- AWS CLI configured
- ECR repository created
- RDS PostgreSQL instance
- ElastiCache Redis (optional)

## Build and Push
```bash
# Build image
aws ecr get-login-password | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
docker build -t fraud-detection .
docker tag fraud-detection:latest $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/fraud-detection:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/fraud-detection:latest
```

## Task Definition (fraud-detection-task.json)
```json
{
  "family": "fraud-detection",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "executionRoleArn": "arn:aws:iam::123456789012:role/ecsTaskExecutionRole",
  "containerDefinitions": [
    {
      "name": "api",
      "image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/fraud-detection:latest",
      "essential": true,
      "portMappings": [
        {
          "containerPort": 8000,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {"name": "ENV", "value": "production"},
        {"name": "LOG_LEVEL", "value": "INFO"}
      ],
      "secrets": [
        {
          "name": "DATABASE_URL",
          "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:fraud-detection/db-url"
        },
        {
          "name": "API_KEY",
          "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:fraud-detection/api-key"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/fraud-detection",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "api"
        }
      }
    },
    {
      "name": "dashboard",
      "image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/fraud-detection:latest",
      "essential": false,
      "portMappings": [
        {
          "containerPort": 8501,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {"name": "ENV", "value": "production"}
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/fraud-detection",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "dashboard"
        }
      }
    }
  ]
}
```

## Deploy with ECS
```bash
# Register task definition
aws ecs register-task-definition --cli-input-json file://fraud-detection-task.json

# Create service
aws ecs create-service \
  --cluster fraud-detection-cluster \
  --service-name fraud-detection-api \
  --task-definition fraud-detection \
  --desired-count 2 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-1234567890abcdef],securityGroups=[sg-1234567890abcdef]}" \
  --load-balancers "targetGroupArn=arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/fraud-detection/abc,containerName=api,containerPort=8000"
```

## RDS Configuration
```bash
# Connect to RDS
export DATABASE_URL="postgresql://username:password@db-instance.us-east-1.rds.amazonaws.com:5432/fraud_detection"
```

## CloudWatch Monitoring
```bash
# Create log group
aws logs create-log-group --log-group-name /ecs/fraud-detection

# View logs
aws logs tail /ecs/fraud-detection --follow
```