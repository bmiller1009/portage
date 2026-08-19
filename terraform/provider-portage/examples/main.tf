terraform {
  required_providers {
    portage = {
      source = "bmiller1009/portage"
    }
  }
}

provider "portage" {
  # api_url also honors $PORTAGE_API_URL if left unset here.
}

resource "portage_execution_profile" "demo" {
  name          = "tf-demo-exec"
  provider_type = "kubernetes"
  config = jsonencode({
    namespace       = "default"
    service_account = "spark"
    image           = "portage/wordcount:0.2.0"
  })
}

resource "portage_storage_profile" "demo" {
  name          = "tf-demo-storage"
  provider_type = "s3"
  config = jsonencode({
    endpoint_url = "http://minio.portage-storage.svc.cluster.local:9000"
  })
  credential_reference = jsonencode({
    provider  = "env"
    reference = "PORTAGE_MINIO"
  })
}

resource "portage_environment" "demo" {
  name                   = "tf-demo-env"
  execution_provider     = "kubernetes"
  execution_profile_name = portage_execution_profile.demo.name
  storage_provider       = "s3"
  storage_profile_name   = portage_storage_profile.demo.name
}
