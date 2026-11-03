# ADR-016: Container Orchestration Evaluation — AWS Fargate vs. Amazon EKS for MercaAndes APIs and PoC Teardown Strategy

- **Status:** Approved (Ready for Review)
- **Date:** 2026-11-03
- **Authors:** Jayzir Martinez (MercaAndes Cloud Infrastructure Team)
- **Related Requirements:** US-06: Deployment of the sellers API on EKS (point-in-time proof of concept; not part of the production architecture) (Tickets: SCRUM-179, SCRUM-180, SCRUM-181, SCRUM-182, SCRUM-183, SCRUM-184)

---

## Context

The MercaAndes Omnichannel Platform operates lightweight, containerized microservices (Marketplace API, Inventory/Sellers API, Promotions API) that expose catalog, order dispatch, and seller stockout data.

In **ADR-012** and **ADR-013**, AWS ECS Fargate was implemented as the serverless execution engine for these APIs, fronted by an Application Load Balancer (ALB). To satisfy the academic/enterprise syllabus requirements of the course (**US-06 / SCRUM-179**), an explicit architectural evaluation was required:
1. **Provisioning EKS (SCRUM-180):** Provision an Amazon EKS cluster with managed worker nodes via Terraform.
2. **Kubernetes Deployment (SCRUM-181):** Package and deploy the Sellers API using declarative Kubernetes manifests (`Namespace`, `ConfigMap`, `Secret`, `Deployment`, `Service`, `HPA`).
3. **Contract Verification (SCRUM-182):** Execute the functional validation suite from HU-2.2 to confirm runtime parity between EKS and Fargate.
4. **Architectural Comparison (SCRUM-183):** Evaluate total cost of ownership (TCO), operational complexity, scalability, and security isolation.
5. **Immediate Teardown (SCRUM-184):** Formulate and execute a teardown strategy to prevent ongoing control plane charges (\$0.10/hour = ~\$73/month).

---

## Decision Drivers

1. **Financial Optimization & Cost Predictability:** Minimizing fixed cloud costs for early-stage and medium-sized workloads.
2. **Operational Overhead:** Limiting maintenance burdens (Kubernetes version upgrades, node AMI patching, CNI plugins) on a lean engineering team.
3. **Runtime & Contract Parity:** Ensuring the Sellers API delivers identical performance, pagination, token validation, and error-handling semantics regardless of container orchestrator.
4. **Security & Workload Isolation:** Strong isolation guarantees between API containers and underlying shared hardware.
5. **Strict Cost Control (Teardown):** Zero dangling cloud spend for temporary proof-of-concept infrastructure.

---

## Considered Options

### Option 1 — Migrate All Production APIs to Amazon EKS
- *Description:* Replace AWS ECS Fargate with a production Amazon EKS cluster hosting all APIs.
- *Trade-offs:* High ecosystem portability (CNCF standards, Helm, GitOps). However, introduces a mandatory fixed fee of **\$73.00/month** for the EKS control plane plus **~\$60.74/month** for a multi-AZ worker node group, totaling **\$133.74/month**. In addition, requires continuous quarterly operational maintenance for Kubernetes lifecycle management.
- *Rejected for Production:* Unjustified overhead for a 3-microservice fleet.

### Option 2 — Standardize Production on AWS ECS Fargate & Use EKS as a Time-Bounded Evaluation PoC (Selected)
- *Description:* Maintain AWS ECS Fargate as the official production runtime for MercaAndes microservices (**\$18.01/month**, serverless, zero maintenance). Implement the EKS deployment strictly as a reproducible, time-bounded proof of concept (PoC) documented in code, validated with automated tests, and destroyed immediately after capturing evidence.
- *Accepted:* Provides the best of both worlds: proves Kubernetes competency and deployment capability, demonstrates 100% contract parity, while protecting the project budget from unnecessary recurring expenses.

---

## Decision

We have decided to:
1. **Maintain AWS ECS Fargate as the primary production container runtime** for all MercaAndes operational APIs (Marketplace, Sellers, Promotions).
2. **Codify Amazon EKS provisioning** in `aws/terraform/eks.tf` and Kubernetes manifests in `k8s/sellers-api/` as a certified, repeatable reference architecture for container orchestration on Kubernetes.
3. **Validate runtime parity** using `k8s/tests/test_sellers_k8s_api.py`, confirming identical responses to HU-2.2 test cases (health checks, token authentication, deterministic pagination, and 429/503 fault tolerance).
4. **Enforce immediate destruction** of the EKS cluster and managed node groups via `k8s/scripts/teardown-eks.ps1` upon completion of evidence gathering to prevent lingering control plane charges.

---

## Consequences

### Positive
* **Cost Efficiency:** Production cloud spend for the API layer remains at **\$18.01/month** instead of escalating to **\$133.74/month** (an 86.5% savings).
* **Zero Operational Burden:** Eliminates the need for ongoing Kubernetes AMI patching, control plane deprecation upgrades, and CNI maintenance.
* **Demonstrated Competency:** Full Kubernetes IaC (Terraform EKS cluster, node groups, OIDC) and cloud-native manifests (`Namespace`, `Deployment`, `Service`, `HPA`) are fully version-controlled in the repository.
* **Automated Cost Protection:** The teardown script ensures clean de-provisioning of all control planes and compute resources.

### Negative / Trade-offs
* **Cloud Lock-in:** ECS Fargate task definitions are specific to AWS, whereas Kubernetes manifests offer multi-cloud portability. However, the codified Kubernetes manifests remain in the repository and can be reactivated should multi-cloud expansion occur in the future.
