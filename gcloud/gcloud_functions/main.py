import requests
import functions_framework
# from shared.gcloud_integration import GCloudIntegration


@functions_framework.http
def main(request):
    # gcloud_integrator = GCloudIntegration()
    # # Sent data to bucket
    # cloud_key = gcloud_integrator.get_secret('deft-melody-404117',
    #                                            'spotify-project-key')
    # gcloud_integrator.upload_data_to_cloud_from_dict()

    SPOTIY_APP_URL = "https://deft-melody-404117.uc.r.appspot.com/get-data"
    response = requests.get(SPOTIY_APP_URL,
                            headers={"Content-Type": "application/json"})
    print(f"Headers: {response.headers}")

    if response.status_code == 200:
        data = response.json()
        if data:
            # Process the fetched data here
            print(data)
            return "Data received and processed successfully"
        else:
            return "No data received from Flask app", 400
    else:
        return f"Error fetching data from Flask app: {response.status_code}", response.status_code
