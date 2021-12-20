# -*- coding: utf-8 -*-

from googleapiclient import discovery, errors
from google.auth import default
from typing import Optional

def getGoogleProjectNumber(project_id: str) -> Optional[str]:
   """Lookup Project Number for gives ProjectID
   
   Args:
      project_id (str): Google Project ID
      
   Returns:
      str: Google Project Number
   
   Uses https://cloud.google.com/resource-manager/reference/rest/v1/projects/get,
   which requires resourcemanager.projects.get permissions.
   """
   
   credentials, _ = default()

   service = discovery.build('cloudresourcemanager', 'v1', credentials=credentials)

   request = service.projects().get(projectId=project_id)
   try:
      response = request.execute()
      return response["projectNumber"]
   except errors.HttpError as e:
      # Unable to resolve project. No permission or project doesn't exist
      # logging.error(f"Cannot resolve projectId {project_id} to project number. Missing 'resourcemanager.projects.get' permissions? ")
      return None
