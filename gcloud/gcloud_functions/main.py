import requests
import functions_framework


@functions_framework.http
def main(request):
    data = request.get_json()
    # Sent data to bucket
    if data:
        # Process the data here
        return "Data received and processed successfully"
    else:
        return "No data received", 400
