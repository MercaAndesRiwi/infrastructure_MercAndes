# ==============================================================================
# Amazon EKS Cluster & Managed Node Group for Sellers API PoC (SCRUM-180)
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. IAM Roles & Policies for EKS Control Plane
# ------------------------------------------------------------------------------
resource "aws_iam_role" "eks_cluster_role" {
  name = "${var.eks_cluster_name}-cluster-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "eks.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Project     = "MercaAndes"
    Component   = "EKS-PoC"
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  role       = aws_iam_role.eks_cluster_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_iam_role_policy_attachment" "eks_vpc_resource_controller" {
  role       = aws_iam_role.eks_cluster_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSVPCResourceController"
}

# ------------------------------------------------------------------------------
# 2. IAM Roles & Policies for EKS Worker Nodes
# ------------------------------------------------------------------------------
resource "aws_iam_role" "eks_node_role" {
  name = "${var.eks_cluster_name}-node-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Project     = "MercaAndes"
    Component   = "EKS-PoC"
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "eks_worker_node_policy" {
  role       = aws_iam_role.eks_node_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "eks_cni_policy" {
  role       = aws_iam_role.eks_node_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "eks_ecr_read_only" {
  role       = aws_iam_role.eks_node_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

# ------------------------------------------------------------------------------
# 3. Security Groups for EKS Cluster and Nodes
# ------------------------------------------------------------------------------
resource "aws_security_group" "eks_cluster_sg" {
  name        = "${var.eks_cluster_name}-cluster-sg"
  description = "Security group for EKS control plane communication with worker nodes"
  vpc_id      = var.vpc_id

  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.eks_cluster_name}-cluster-sg"
    Project     = "MercaAndes"
    Component   = "EKS-PoC"
    Environment = var.environment
  }
}

resource "aws_security_group" "eks_nodes_sg" {
  name        = "${var.eks_cluster_name}-nodes-sg"
  description = "Security group for EKS worker nodes and Pods"
  vpc_id      = var.vpc_id

  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.eks_cluster_name}-nodes-sg"
    Project     = "MercaAndes"
    Component   = "EKS-PoC"
    Environment = var.environment
  }
}

# Allow nodes to communicate with cluster API
resource "aws_security_group_rule" "cluster_ingress_nodes" {
  type                     = "ingress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  security_group_id        = aws_security_group.eks_cluster_sg.id
  source_security_group_id = aws_security_group.eks_nodes_sg.id
  description              = "Allow worker nodes to reach EKS API server"
}

# Allow cluster API to communicate with nodes (kubelet)
resource "aws_security_group_rule" "nodes_ingress_cluster" {
  type                     = "ingress"
  from_port                = 10250
  to_port                  = 10250
  protocol                 = "tcp"
  security_group_id        = aws_security_group.eks_nodes_sg.id
  source_security_group_id = aws_security_group.eks_cluster_sg.id
  description              = "Allow EKS control plane to reach worker node kubelets"
}

# Allow inter-pod communication
resource "aws_security_group_rule" "nodes_ingress_self" {
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.eks_nodes_sg.id
  self              = true
  description       = "Allow pods on worker nodes to communicate with each other"
}

# ------------------------------------------------------------------------------
# 4. Amazon EKS Cluster Definition (Control Plane)
# ------------------------------------------------------------------------------
resource "aws_eks_cluster" "poc" {
  name     = var.eks_cluster_name
  role_arn = aws_iam_role.eks_cluster_role.arn
  version  = var.eks_cluster_version

  vpc_config {
    subnet_ids              = var.eks_subnet_ids
    security_group_ids      = [aws_security_group.eks_cluster_sg.id]
    endpoint_private_access = true
    endpoint_public_access  = true
  }

  enabled_cluster_log_types = ["api", "audit", "authenticator", "controllerManager", "scheduler"]

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy,
    aws_iam_role_policy_attachment.eks_vpc_resource_controller
  ]

  tags = {
    Project     = "MercaAndes"
    Component   = "EKS-PoC"
    Environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# 5. Amazon EKS Managed Node Group (Worker Nodes)
# ------------------------------------------------------------------------------
resource "aws_eks_node_group" "poc_nodes" {
  cluster_name    = aws_eks_cluster.poc.name
  node_group_name = "${var.eks_cluster_name}-nodegroup"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  subnet_ids      = var.eks_subnet_ids

  instance_types = var.eks_node_instance_types
  capacity_type  = "SPOT" # Cost-effective spot instances for PoC evaluation

  scaling_config {
    desired_size = var.eks_node_desired_size
    min_size     = var.eks_node_min_size
    max_size     = var.eks_node_max_size
  }

  update_config {
    max_unavailable = 1
  }

  labels = {
    role        = "poc-worker"
    environment = var.environment
    workload    = "sellers-api"
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node_policy,
    aws_iam_role_policy_attachment.eks_cni_policy,
    aws_iam_role_policy_attachment.eks_ecr_read_only,
    aws_eks_cluster.poc
  ]

  tags = {
    Project     = "MercaAndes"
    Component   = "EKS-PoC"
    Environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# 6. OIDC Provider for IAM Roles for Service Accounts (IRSA)
# ------------------------------------------------------------------------------
data "tls_certificate" "eks_oidc" {
  url = aws_eks_cluster.poc.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks_oidc" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks_oidc.certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.poc.identity[0].oidc[0].issuer

  tags = {
    Project     = "MercaAndes"
    Component   = "EKS-PoC"
    Environment = var.environment
  }
}
