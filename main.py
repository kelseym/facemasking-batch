import time

import requests
import xnat
import pandas

xnatpy_session = None
requests_session = None


def get_xnat_session(xnat_host, username, password):
    # Create an XNAT session
    global xnatpy_session
    xnatpy_session= xnatpy_session if xnatpy_session else xnat.connect(xnat_host, user=username, password=password)
    return xnatpy_session


def get_requests_session():
    global requests_session
    requests_session = requests_session if requests_session else requests.Session()
    return requests_session


def get_session_project_scans(http_session, project_id, session_label, ref_scan_id, scan2id, scan3id, scan4id, scan5id):
    # Retrieve session, xnat_project, and ref_scan1 information programmatically
    xnat_project = http_session.projects[project_id]
    project_uri = xnat_project.uri.replace("/data/", "/archive/")
    session = xnat_project.experiments[session_label]
    session_uri = "/archive/experiments/" + session.id

    return session_uri, project_uri, session_uri + "/scans/" + ref_scan_id, \
                                     session_uri + "/scans/" + scan2id if scan2id else '', \
                                     session_uri + "/scans/" + scan3id if scan3id else '', \
                                     session_uri + "/scans/" + scan4id if scan4id else '', \
                                     session_uri + "/scans/" + scan5id if scan5id else ''


def launch_facemasking_on_xnat(xnat_host, username, password, project_id, session_id, ref_scan_id, scan2id, scan3id,
                               scan4id, scan5id):
    http_session = get_xnat_session(xnat_host, username, password)

    # Get session, project, and ref_scan1 URIs
    session_uri, project_uri, ref_scan_uri, scan2_uri, scan3_uri, scan4_uri, scan5_uri = \
        get_session_project_scans(http_session, project_id, session_id, ref_scan_id, scan2id, scan3id, scan4id, scan5id)

    # REST API endpoint for session launch
    api_endpoint = f"{xnat_host}/xapi/projects/M19021_glioma2/wrappers/230/root/session/launch"

    # JSON request object
    request_object = {
        "session": session_uri,
        "project": project_uri,
        "ref_scan1": ref_scan_uri,
        "scan2": scan2_uri,
        "scan3": scan3_uri,
        "scan4": scan4_uri,
        "scan5": scan5_uri
    }

    # Make the API call
    response = get_requests_session().post(api_endpoint, json=request_object, auth=(username, password))

    if response.status_code == 200:
        print("Session launched successfully!")
    else:
        print(f"Failed to launch session. Status code: {response.status_code}")
        print(response.text)


# Replace host, username, password and csv file name
xnat_host_url = "https://mirrir.wustl.edu"
username = "your_username"
password = "your_password"
csv_file = 'facemask_batch_input_test.csv'  # Replace with your CSV file path

df = pandas.read_csv(csv_file, delimiter=',')  # Adjust the delimiter if needed


# Iterate through the rows of the DataFrame
for index, row in df.iterrows():
    time.sleep(5)
    # Extract values from the current row
    subject = row['MIRRIR_subj']
    project = row['project']
    session_label = row['MIRRIR_session']
    ref_scan_id = row['ref_scan']
    scan_series = row['scan_series']

    # Call the function to launch the facemasking container using groups of four scan IDs from scan_series
    scan_ids = str(scan_series).split(',')
    grouped_scan_ids = [scan_ids[i:i + 4] for i in range(0, len(scan_ids), 4)]
    for i, scan_group in enumerate(grouped_scan_ids, start=1):
        launch_facemasking_on_xnat(xnat_host_url, username, password, project, session_label, str(ref_scan_id),
                                   scan_group[0],
                                   scan_group[1] if len(scan_group) > 1 else None,
                                   scan_group[2] if len(scan_group) > 2 else None,
                                   scan_group[3] if len(scan_group) > 3 else None)
