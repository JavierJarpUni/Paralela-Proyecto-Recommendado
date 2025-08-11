#!/bin/bash
# deploy.sh

set -e

PROJECT_ID="your-project-id"
REGION="us-central1"

echo "🚀 Deploying BigData Hybrid Application..."

# 1. Build and deploy Actor System to Cloud Run
echo "📦 Building Actor System..."
sbt clean compile docker:publishLocal

# Tag for GCR
docker tag bigdata-actor-system:0.1.0-SNAPSHOT gcr.io/$PROJECT_ID/bigdata-actor-system:latest

# Push to GCR
echo "⬆️ Pushing to Container Registry..."
docker push gcr.io/$PROJECT_ID/bigdata-actor-system:latest

# 2. Package Cloud Functions
echo "📦 Packaging Cloud Functions..."
mkdir -p dist/cloud-functions

# API Gateway
cd functions/api-gateway
zip -r ../../dist/cloud-functions/api-gateway.zip .
cd ../..

# Metal Processor
cd functions/metal-processor  
zip -r ../../dist/cloud-functions/metal-processor.zip .
cd ../..

# Upload to Cloud Storage
gsutil cp dist/cloud-functions/*.zip gs://$PROJECT_ID-bigdata-processing-results/cloud-functions/

# 3. Deploy Spark JARs
echo "📦 Building Spark JARs..."
sbt assembly
gsutil cp target/scala-2.12/bigdata-processor-assembly-0.1.0-SNAPSHOT.jar gs://$PROJECT_ID-bigdata-processing-results/spark-jars/bigdata-processor.jar

# 4. Create init script for Dataproc
cat > /tmp/setup-spark.sh << 'EOF'
#!/bin/bash
# Download custom JARs
gsutil cp gs://PROJECT_ID-bigdata-processing-results/spark-jars/* /usr/lib/spark/jars/
EOF

sed "s/PROJECT_ID/$PROJECT_ID/g" /tmp/setup-spark.sh > /tmp/setup-spark-final.sh
gsutil cp /tmp/setup-spark-final.sh gs://$PROJECT_ID-bigdata-processing-results/init-scripts/setup-spark.sh

# 5. Deploy infrastructure with Terraform
echo "🏗️ Deploying Infrastructure..."
cd terraform
terraform init
terraform plan -var="project_id=$PROJECT_ID" -var="region=$REGION"
terraform apply -var="project_id=$PROJECT_ID" -var="region=$REGION" -auto-approve
cd ..

echo "✅ Deployment completed!"
echo "📍 API Gateway URL: $(terraform -chdir=terraform output -raw api_gateway_url)"