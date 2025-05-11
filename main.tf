terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-east-1" # Updated to an appropriate region for M6a instances
}

resource "aws_key_pair" "generated_key" {
  key_name   = "1brc-generated-key" # Automatically generated key pair
  public_key = file("C:/Users/ompur/.ssh/id_rsa.pub") # Updated path for Windows
}
resource "aws_security_group" "allow_ssh" {
  name_prefix = "allow_ssh"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Allow SSH from anywhere (use cautiously)
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
resource "aws_instance" "app_server" {
  ami           = "ami-084568db4383264d4" # Correct Ubuntu Server 24.04 LTS AMI for us-east-1
  instance_type = "m6a.8xlarge" # Updated instance type

  key_name = aws_key_pair.generated_key.key_name # Reference the generated key pair
  security_groups = [aws_security_group.allow_ssh.name] # Attach the security group

  tags = {
    Name = "1brc-test-run" # Updated name tag
  }
}
