resource "google_cloudfunctions_function" "API_service" {
    available_memory_mb   = 256
    entry_point           = "API_service"
    environment_variables = {}
    ingress_settings      = "ALLOW_INTERNAL_ONLY"

    max_instances         = 1
    name                  = "API_service"
    project               = "PROJECT_ID"
    region                = "europe-west2"
    runtime               = "python38"
    service_account_email = "SERVICE_ACCOUNT_EMAIL"
    timeout               = 60
    trigger_http          = true
    
    source_repository {
        url          = "https://source.developers.google.com/projects/PROJECT_ID/repos/NAME_REPO/moveable-aliases/master/paths/FOLDER/PATH/TO_CF"
    }

    timeouts {}

}