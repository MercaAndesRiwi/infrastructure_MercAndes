# MercaAndes EKS Sellers API Proof of Concept (US-06 / SCRUM-179)

This directory contains the Kubernetes manifests, integration test suites, and automated teardown automation for the **Sellers API** (Seller Inventory Simulator) proof of concept on **Amazon Elastic Kubernetes Service (EKS)**.

---

## Architecture & Directory Structure

```text
infrastructure_MercAndes/
├── aws/
│   └── terraform/
│       ├── eks.tf               # Terraform module: EKS cluster, managed spot node group, IAM & SGs
│       ├── variables.tf         # Parameterized cluster version, node instance types, and subnets
│       └── outputs.tf           # Cluster endpoint, ARN, and OIDC provider outputs
├── k8s/
│   ├── sellers-api/
│   │   ├── 00-namespace.yaml    # Namespace: mercaandes-sellers
│   │   ├── 01-configmap-secrets.yaml # ConfigMap and Secret for runtime environment & DB credentials
│   │   ├── 02-deployment.yaml   # 2-replica Deployment with liveness/readiness probes & resource limits
│   │   ├── 03-service.yaml      # Internal ClusterIP & optional external LoadBalancer Service
│   │   └── 04-hpa.yaml          # HorizontalPodAutoscaler (CPU 70%, Memory 80%, 2-5 replicas)
│   ├── tests/
│   │   └── test_sellers_k8s_api.py # Automated HU-2.2 contract & runtime parity test suite
│   └── scripts/
│       └── teardown-eks.ps1     # Automated cluster teardown script to stop hourly control plane billing
└── docs/
    ├── fargate_vs_eks_comparison.md # Detailed 5-pillar technical benchmark & TCO analysis
    └── ADR-016-container-orchestration-fargate-vs-eks.md # Formal Architecture Decision Record
```

---

## 1. Provisioning the EKS Cluster (SCRUM-180)

The EKS cluster is provisioned via Terraform into the existing private VPC subnets:

```bash
cd aws/terraform
terraform init
terraform plan -target=aws_eks_cluster.poc -target=aws_eks_node_group.poc_nodes
terraform apply -target=aws_eks_cluster.poc -target=aws_eks_node_group.poc_nodes
```

Update your local kubeconfig:
```bash
aws eks update-kubeconfig --region us-east-1 --name mercaandes-eks-poc
```

---

## 2. Deploying the Sellers API Workload (SCRUM-181)

Deploy all manifests in sequential order:

```bash
kubectl apply -f k8s/sellers-api/
```

Verify pod health and replica distribution:
```bash
kubectl get pods -n mercaandes-sellers -o wide
kubectl get svc -n mercaandes-sellers
kubectl get hpa -n mercaandes-sellers
```

---

## 3. Running Functional & Contract Verification Tests (SCRUM-182)

Port-forward the service to localhost (or use the LoadBalancer endpoint):
```bash
kubectl port-forward svc/sellers-api-service 8000:80 -n mercaandes-sellers
```

Execute the test suite verifying complete parity with HU-2.2:
```bash
pytest k8s/tests/test_sellers_k8s_api.py -v
```

---

## 4. Architectural Findings & ADR (SCRUM-183)

Read the comprehensive technical comparison and ADR:
* [Fargate vs. EKS Benchmark Report](../docs/fargate_vs_eks_comparison.md)
* [ADR-016 Confluence Page](https://la-sierrita.atlassian.net/wiki/spaces/D/pages/21004289/ADR-016+Container+Orchestration+Evaluation+-+AWS+Fargate+vs+Amazon+EKS+and+PoC+Teardown)

---

## 5. Immediate Teardown & Cost Mitigation (SCRUM-184)

To prevent ongoing hourly control plane charges ($0.10/hour = ~$73/month):

```powershell
.\k8s\scripts\teardown-eks.ps1
```
