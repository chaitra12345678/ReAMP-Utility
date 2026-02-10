"""
ServiceNow Connector Module
Handles integration with ServiceNow API for incident management
"""

import os
import base64
import json
import logging
from typing import List, Dict, Optional
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

logger = logging.getLogger(__name__)

class ServiceNowConnector:
    """
    Connector for ServiceNow REST API
    Handles authentication, incident retrieval, and updates
    """
    
    def __init__(self, 
                 base_url: str = None,
                 username: str = None,
                 password: str = None,
                 dry_run: bool = False):
        """
        Initialize ServiceNow connector
        
        Args:
            base_url: ServiceNow instance URL
            username: API user username
            password: API user password
            dry_run: If True, don't make actual updates (for testing)
        """
        self.base_url = base_url or os.getenv('SERVICENOW_URL', 'https://dev324542.service-now.com')
        self.username = username or os.getenv('SERVICENOW_USER', 'admin')
        self.password = password or os.getenv('SERVICENOW_PASSWORD', 'Vj97WxJX=r*t')
        self.dry_run = dry_run
        
        self.auth = HTTPBasicAuth(self.username, self.password)
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        logger.info(f"ServiceNow Connector initialized: {self.base_url}")
    
    def _check_connection(self) -> bool:
        """Verify connection to ServiceNow"""
        try:
            url = f"{self.base_url}/api/now/table/incident"
            params = {'sysparm_limit': 1}
            response = requests.get(
                url, 
                auth=self.auth, 
                params=params, 
                headers=self.headers,
                timeout=5
            )
            
            if response.status_code == 200:
                logger.info("✓ ServiceNow connection verified")
                return True
            else:
                logger.error(f"✗ ServiceNow connection failed: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"✗ ServiceNow connection error: {str(e)}")
            return False
    
    def get_new_incidents(self, limit: int = 50) -> List[Dict]:
        """
        Fetch unprocessed incidents from ServiceNow
        
        Args:
            limit: Maximum number of incidents to retrieve
            
        Returns:
            List of incident dictionaries
        """
        try:
            url = f"{self.base_url}/api/now/table/incident"
            
            # Query for new, unassigned incidents
            params = {
                'sysparm_query': 'state=1^ORstate=2^assignment_groupISEMPTY',
                'sysparm_limit': limit,
                'sysparm_fields': 'sys_id,number,short_description,description,state,priority,category',
                'sysparm_exclude_reference_link': 'true'
            }
            
            response = requests.get(
                url,
                auth=self.auth,
                params=params,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                results = response.json().get('result', [])
                logger.info(f"✓ Retrieved {len(results)} incidents from ServiceNow")
                return results
            else:
                logger.error(f"✗ Failed to get incidents: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"✗ Error retrieving incidents: {str(e)}")
            return []

    def get_category_choices(self) -> List[str]:
        """
        Fetch available choices for the Incident 'category' field from ServiceNow
        Returns a list of choice `value`s (strings)
        """
        try:
            url = f"{self.base_url}/api/now/table/sys_choice"
            params = {
                'sysparm_query': 'name=incident^element=category',
                'sysparm_fields': 'value,label',
                'sysparm_limit': 1000
            }
            response = requests.get(
                url,
                auth=self.auth,
                params=params,
                headers=self.headers,
                timeout=10
            )
            if response.status_code == 200:
                results = response.json().get('result', [])
                # Prefer the 'value' field (the actual stored db value), but return label if value missing
                choices = [r.get('value') or r.get('label') for r in results if r]
                logger.info(f"✓ Retrieved {len(choices)} category choices from ServiceNow")
                return choices
            else:
                logger.error(f"✗ Failed to fetch category choices: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"✗ Error fetching category choices: {str(e)}")
            return []

    def get_choice_values(self, element: str) -> List[str]:
        """
        Fetch choices for an Incident field from sys_choice.
        Returns list of 'value' strings.
        """
        try:
            url = f"{self.base_url}/api/now/table/sys_choice"
            params = {
                "sysparm_query": f"name=incident^element={element}",
                "sysparm_fields": "value,label",
                "sysparm_limit": 1000
            }
            response = requests.get(
                url,
                auth=self.auth,
                params=params,
                headers=self.headers,
                timeout=10
            )
            if response.status_code == 200:
                results = response.json().get("result", [])
                values = [r.get("value") or r.get("label") for r in results if r]
                return list(dict.fromkeys(values))
            logger.error(f"✗ Failed to fetch choices for {element}: {response.status_code}")
            return []
        except Exception as e:
            logger.error(f"✗ Error fetching choices for {element}: {str(e)}")
            return []

    def get_priority_lookup_values(self) -> Dict[str, List[str]]:
        """
        Fetch impact/urgency/priority values from dl_u_priority.
        """
        try:
            url = f"{self.base_url}/api/now/table/dl_u_priority"
            params = {
                "sysparm_fields": "impact,urgency,priority",
                "sysparm_limit": 1000,
                "sysparm_exclude_reference_link": "true"
            }
            response = requests.get(
                url,
                auth=self.auth,
                params=params,
                headers=self.headers,
                timeout=10
            )
            if response.status_code == 200:
                results = response.json().get("result", [])
                impacts = sorted({r.get("impact") for r in results if r.get("impact")})
                urgencies = sorted({r.get("urgency") for r in results if r.get("urgency")})
                priorities = sorted({r.get("priority") for r in results if r.get("priority")})
                return {
                    "impact": impacts,
                    "urgency": urgencies,
                    "priority": priorities
                }
            logger.error(f"✗ Failed to fetch priority lookup: {response.status_code}")
            return {"impact": [], "urgency": [], "priority": []}
        except Exception as e:
            logger.error(f"✗ Error fetching priority lookup: {str(e)}")
            return {"impact": [], "urgency": [], "priority": []}
    
    def update_incident(self,
                       ticket_id: str,
                       category: str,
                       confidence: float,
                       assignment_group: str = None,
                       work_notes: str = None,
                       snow_category: str = None,
                       subcategory: str = None,
                       impact: str = None,
                       urgency: str = None) -> bool:
        """
        Update incident with classification results
        
        Args:
            ticket_id: ServiceNow incident sys_id
            category: Classified category (internal)
            confidence: Confidence score (0-1)
            assignment_group: Team to assign to
            work_notes: Additional notes
            snow_category: Value to set in ServiceNow's `incident.category` field (string)
            
        Returns:
            True if update successful, False otherwise
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would update {ticket_id}: {category} ({confidence:.2%}) (set category: {snow_category})")
            return True
        
        try:
            url = f"{self.base_url}/api/now/table/incident/{ticket_id}"
            
            update_data = {
                'u_auto_classification_category': category,
                'u_confidence_score': str(round(confidence * 100, 2)),
                'u_auto_classified': 'true',
                'u_classification_timestamp': datetime.utcnow().isoformat()
            }
            
            # Set assignment group if provided
            if assignment_group:
                update_data['assignment_group'] = assignment_group
            
            # Set ServiceNow's standard 'category' field when provided
            if snow_category:
                # Ensure ServiceNow category fits constraints (max length 40)
                if isinstance(snow_category, str):
                    update_data['category'] = snow_category[:40]

            if subcategory:
                update_data['subcategory'] = subcategory
            if impact:
                update_data['impact'] = impact
            if urgency:
                update_data['urgency'] = urgency
            
            if work_notes:
                update_data['work_notes'] = work_notes
            else:
                update_data['work_notes'] = f"Auto-classified as {category} (confidence: {confidence:.1%}) by Task Classifier"
            
            response = requests.patch(
                url,
                auth=self.auth,
                json=update_data,
                headers=self.headers,
                timeout=10
            )

            if response.status_code == 200:
                logger.info(f"✓ Updated {ticket_id}: {category} ({confidence:.1%}) (category set: {update_data.get('category')})")
                return True
            else:
                # Log response body for debugging (best-effort)
                body = response.text if hasattr(response, 'text') else '<no body>'
                logger.error(f"✗ Failed to update {ticket_id}: {response.status_code} - {body}")
                return False
                
        except Exception as e:
            logger.error(f"✗ Error updating incident: {str(e)}")
            return False
    
    def get_assignment_group_id(self, group_name: str) -> Optional[str]:
        """
        Get sys_id for assignment group name
        
        Args:
            group_name: Name of the assignment group
            
        Returns:
            sys_id of the group, or None if not found
        """
        try:
            url = f"{self.base_url}/api/now/table/sys_user_group"
            params = {
                'sysparm_query': f'name={group_name}',
                'sysparm_fields': 'sys_id,name'
            }
            
            response = requests.get(
                url,
                auth=self.auth,
                params=params,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                results = response.json().get('result', [])
                if results:
                    return results[0]['sys_id']
            
            return None
            
        except Exception as e:
            logger.error(f"✗ Error getting assignment group: {str(e)}")
            return None


class MockServiceNowConnector(ServiceNowConnector):
    """
    Mock ServiceNow connector for testing
    Simulates ServiceNow behavior without actual connection
    """
    
    def __init__(self, test_data: List[Dict] = None):
        """Initialize with mock data"""
        super().__init__(dry_run=True)
        self.test_data = test_data or []
        self.updates = []
        logger.info("✓ Mock ServiceNow Connector initialized (for testing)")
    
    def _check_connection(self) -> bool:
        logger.info("✓ Mock connection verified")
        return True
    
    def get_new_incidents(self, limit: int = 50) -> List[Dict]:
        """Return mock test data"""
        logger.info(f"✓ Mock: Retrieved {len(self.test_data)} mock incidents")
        return self.test_data[:limit]
    
    def update_incident(self,
                       ticket_id: str,
                       category: str,
                       confidence: float,
                       assignment_group: str = None,
                       work_notes: str = None,
                       snow_category: str = None) -> bool:
        """Store update for verification"""
        self.updates.append({
            'ticket_id': ticket_id,
            'category': category,
            'confidence': confidence,
            'assignment_group': assignment_group,
            'snow_category': snow_category,
            'timestamp': datetime.utcnow().isoformat()
        })
        logger.info(f"✓ Mock updated {ticket_id}: {category} ({confidence:.1%}) (category set: {snow_category})")
        return True
    
    def get_updates(self) -> List[Dict]:
        """Get all recorded updates"""
        return self.updates
