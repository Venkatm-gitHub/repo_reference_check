
from version import VERSION
import requests # Contains methods used to make HTTP requests
import xml.etree.ElementTree as ET # Contains methods used to build and parse XML
import sys
import os
import math
import getpass

# The following packages are used to build a multi-part/mixed request.
# They are contained in the 'requests' library
from requests.packages.urllib3.fields import RequestField
from requests.packages.urllib3.filepost import encode_multipart_formdata

# The namespace for the REST API is 'http://tableausoftware.com/api' for Tableau Server 9.0
# or 'http://tableau.com/api' for Tableau Server 9.1 or later
xmlns = {'t': 'http://tableau.com/api'}

# The maximum size of a file that can be published in a single request is 64MB
FILESIZE_LIMIT = 1024 * 1024 * 64   # 64MB

# For when a file is over 64MB, break it into 5MB(standard chunk size) chunks
CHUNK_SIZE = 1024 * 1024 * 5    # 5MB

# If using python version 3.x, 'raw_input()' is changed to 'input()'
if sys.version[0] == '3': raw_input=input


class ApiCallError(Exception):
    pass


class UserDefinedFieldError(Exception):
    pass


def _encode_for_display(text):
    """
    Encodes strings so they can display as ASCII in a Windows terminal window.
    This function also encodes strings for processing by xml.etree.ElementTree functions. 
    
    Returns an ASCII-encoded version of the text.
    Unicode characters are converted to ASCII placeholders (for example, "?").
    """
    return text.encode('ascii', errors="backslashreplace").decode('utf-8')


def _make_multipart(parts):
    """
    Creates one "chunk" for a multi-part upload
    'parts' is a dictionary that provides key-value pairs of the format name: (filename, body, content_type).
    Returns the post body and the content type string.
    For more information, see this post:
        http://stackoverflow.com/questions/26299889/how-to-post-multipart-list-of-json-xml-files-using-python-requests
    """
    mime_multipart_parts = []
    for name, (filename, blob, content_type) in parts.items():
        multipart_part = RequestField(name=name, data=blob, filename=filename)
        multipart_part.make_multipart(content_type=content_type)
        mime_multipart_parts.append(multipart_part)

    post_body, content_type = encode_multipart_formdata(mime_multipart_parts)
    content_type = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
    return post_body, content_type


def _check_status(server_response, success_code):
    """
    Checks the server response for possible errors.
    'server_response'       the response received from the server
    'success_code'          the expected success code for the response
    Throws an ApiCallError exception if the API call fails.
    """
    if server_response.status_code != success_code:
        parsed_response = ET.fromstring(server_response.text)

        # Obtain the 3 xml tags from the response: error, summary, and detail tags
        error_element = parsed_response.find('t:error', namespaces=xmlns)
        summary_element = parsed_response.find('.//t:summary', namespaces=xmlns)
        detail_element = parsed_response.find('.//t:detail', namespaces=xmlns)

        # Retrieve the error code, summary, and detail if the response contains them
        code = error_element.get('code', 'unknown') if error_element is not None else 'unknown code'
        summary = summary_element.text if summary_element is not None else 'unknown summary'
        detail = detail_element.text if detail_element is not None else 'unknown detail'
        error_message = '{0}: {1} - {2}'.format(code, summary, detail)
        raise ApiCallError(error_message)
    return


def sign_in(server, username, password, site=""):
    """
    Signs in to the server specified with the given credentials
    'server'   specified server address
    'username' is the name (not ID) of the user to sign in as.
               Note that most of the functions in this example require that the user
               have server administrator permissions.
    'password' is the password for the user.
    'site'     is the ID (as a string) of the site on the server to sign in to. The
               default is "", which signs in to the default site.
    Returns the authentication token and the site ID.
    """
    url = server + "/api/{0}/auth/signin".format(VERSION)

    # Builds the request
    xml_request = ET.Element('tsRequest')
    credentials_element = ET.SubElement(xml_request, 'credentials', name=username, password=password)
    ET.SubElement(credentials_element, 'site', contentUrl=site)
    xml_request = ET.tostring(xml_request)

    # Make the request to server
    server_response = requests.post(url, data=xml_request,verify=False)
    _check_status(server_response, 200)

    # ASCII encode server response to enable displaying to console
    server_response = _encode_for_display(server_response.text)

    # Reads and parses the response
    parsed_response = ET.fromstring(server_response)

    # Gets the auth token and site ID
    token = parsed_response.find('t:credentials', namespaces=xmlns).get('token')
    site_id = parsed_response.find('.//t:site', namespaces=xmlns).get('id')
    return token, site_id


def sign_out(server, auth_token):
    """
    Destroys the active session and invalidates authentication token.
    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    """
    url = server + "/api/{0}/auth/signout".format(VERSION)
    server_response = requests.post(url, headers={'x-tableau-auth': auth_token},verify=False)
    _check_status(server_response, 204)
    return


def start_upload_session(server, auth_token, site_id):
    """
    Creates a POST request that initiates a file upload session.
    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'site_id'       ID of the site that the user is signed into
    Returns a session ID that is used by subsequent functions to identify the upload session.
    """
    url = server + "/api/{0}/sites/{1}/fileUploads".format(VERSION, site_id)
    server_response = requests.post(url, headers={'x-tableau-auth': auth_token},verify=False)
    _check_status(server_response, 201)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    return xml_response.find('t:fileUpload', namespaces=xmlns).get('uploadSessionId')


def get_default_project_id(server, auth_token, site_id):
    """
    Returns the project ID for the 'default' project on the Tableau server.
    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'site_id'       ID of the site that the user is signed into
    """
    page_num, page_size = 1, 100   # Default paginating values

    # Builds the request
    url = server + "/api/{0}/sites/{1}/projects".format(VERSION, site_id)
    paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page_num)
    server_response = requests.get(paged_url, headers={'x-tableau-auth': auth_token},verify=False)
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))

    # Used to determine if more requests are required to find all projects on server
    total_projects = int(xml_response.find('t:pagination', namespaces=xmlns).get('totalAvailable'))
    max_page = int(math.ceil(total_projects / page_size))

    projects = xml_response.findall('.//t:project', namespaces=xmlns)

    # Continue querying if more projects exist on the server
    for page in range(2, max_page + 1):
        paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page)
        server_response = requests.get(paged_url, headers={'x-tableau-auth': auth_token})
        _check_status(server_response, 200)
        xml_response = ET.fromstring(_encode_for_display(server_response.text))
        projects.extend(xml_response.findall('.//t:project', namespaces=xmlns))

    # Look through all projects to find the 'default' one
    for project in projects:
        if project.get('name') == 'default' or project.get('name') == 'Default':
            return project.get('id')
    raise LookupError("Project named 'default' was not found on server")


def main():
        
    server = "https://la-tab01.dev.wkelms.com"  
    site = "kevin"
    username = "admin"
    password = "p@ssw0rd!"
    file_name_path = "//houfile01/public/Engineering/_LA/tableau/publish/DEV041/datasources/Invoice Fact With Security.tdsx"
    
    file_name_path = os.path.abspath(file_name_path)

    # Datasource file with extension, without full path
    file_name_ext = os.path.basename(file_name_path)

    print("\n*Publishing '{0}' to the default project as {1}*".format(file_name_ext, username))
    
    if not os.path.isfile(file_name_path):
        error = "{0}: file not found".format(file_name_path)
        raise IOError(error)

    # Break datasource file by name and extension
    file_name, file_extension = file_name_ext.split('.', 1)

    if file_extension != 'tdsx':
        error = "This sample only accepts .tdsx files to publish. More information in file comments."
        raise UserDefinedFieldError(error)

    # Get size to check if chunking is necessary
    file_size = os.path.getsize(file_name_path)
    chunked = True #file_size >= FILESIZE_LIMIT

    ##### STEP 1: SIGN IN #####
    print("\n1. Signing in as " + username)
    auth_token, site_id = sign_in(server, username, password, site)

    ##### STEP 2: OBTAIN DEFAULT PROJECT ID #####
    print("\n2. Finding the 'default' project to publish to")
    project_id = get_default_project_id(server, auth_token, site_id)

    ##### STEP 3: PUBLISH ######
    # Build a general request for publishing
    xml_request = ET.Element('tsRequest')
    element = ET.SubElement(xml_request, 'datasources', name=file_name)
    ET.SubElement(element, 'project', id=project_id)
    xml_request = ET.tostring(xml_request)

    if chunked:
        print("\n3. Publishing '{0}' in {1}MB chunks (file over 64MB)".format(file_name, CHUNK_SIZE / 1024000))
        # Initiates an upload session
        uploadID = start_upload_session(server, auth_token, site_id)

        # URL for PUT request to append chunks for publishing
        put_url = server + "/api/{0}/sites/{1}/fileUploads/{2}".format(VERSION, site_id, uploadID)

        # Read the contents of the file in chunks of 100KB
        with open(file_name_path, 'rb') as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                payload, content_type = _make_multipart({'request_payload': ('', '', 'text/xml'),
                                                         'tableau_file': ('file', data, 'application/octet-stream')})
                print("\tPublishing a chunk...")
                server_response = requests.put(put_url, data=payload,
                                               headers={'x-tableau-auth': auth_token, "content-type": content_type}, verify=False)
                _check_status(server_response, 200)

        # Finish building request for chunking method
        payload, content_type = _make_multipart({'request_payload': ('', xml_request, 'text/xml')})

        publish_url = server + "/api/{0}/sites/{1}/datasources".format(VERSION, site_id)
        publish_url += "?uploadSessionId={0}".format(uploadID)
        publish_url += "&datasourceType={0}&overwrite=true".format(file_extension)
    else:
        print("\n3. Publishing '" + file_name + "' using the all-in-one method (datasources under 64MB)")
        # Read the contents of the file to publish
        with open(file_name_path, 'rb') as f:
            bytes = f.read()

        # Finish building request for all-in-one method
        parts = {'request_payload': ('', xml_request, 'text/xml'),
                 'tableau_datasources': (file_name, bytes, 'application/octet-stream')}
        payload, content_type = _make_multipart(parts)

        publish_url = server + "/api/{0}/sites/{1}/datasources".format(VERSION, site_id)
        publish_url += "?datasourceType={0}&overwrite=true".format(file_extension)

    # Make the request to publish and check status code
    print("\tUploading...")
    server_response = requests.post(publish_url, data=payload,
                                    headers={'x-tableau-auth': auth_token, 'content-type': content_type},verify=False)

    _check_status(server_response, 201)

    ##### STEP 4: SIGN OUT #####
    print("\n4. Signing out, and invalidating the authentication token")
    sign_out(server, auth_token)
    auth_token = None


if __name__ == '__main__':
    main()