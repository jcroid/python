resource "google_cloudfunctions_function" "customer_lifecycle" {
    available_memory_mb   = 256
    entry_point           = "customer_lifecycle"
    environment_variables = {}
    ingress_settings      = "ALLOW_INTERNAL_ONLY"

    max_instances         = 1
    name                  = "customer_lifecycle"
    project               = "PROJECT_ID"
    region                = "europe-west1"
    runtime               = "python37"
    service_account_email = "SERVICE_ACCOUNT_EMAIL"
    timeout               = 60
    trigger_http          = true
    
    source_repository {
        url          = "https://source.developers.google.com/projects/PROJECT_ID/repos/NAME_REPO/moveable-aliases/master/paths/FOLDER/PATH/TO_CF"
    }

    timeouts {}

}