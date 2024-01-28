import requests
import functions_framework
#from shared.gcloud_integration import GCloudIntegration

@functions_framework.http
def main(request):
    # gcloud_integrator = GCloudIntegration()
    # # Sent data to bucket
    # cloud_key = gcloud_integrator.get_secret('deft-melody-404117',
    #                                            'spotify-project-key')
    # gcloud_integrator.upload_data_to_cloud_from_dict()
    data = request.get_json()
    if data:
        # Process the data here
        return "Data received and processed successfully"
    else:
        return "No data received", 400
