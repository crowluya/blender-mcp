"""
Proxy connector for Blender MCP.
Uses HTTP/HTTPS to connect to a Blender proxy running on the user's machine.
"""

import json
import time
import logging
import requests
import uuid
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from .base import BlenderConnector

logger = logging.getLogger("BlenderMCPServer")

class ProxyConnector(BlenderConnector):
    """
    Proxy connector that uses HTTP/HTTPS to connect to a Blender proxy.
    This allows secure communication with a local Blender instance through the internet.
    """
    
    def __init__(
        self, 
        proxy_url: str, 
        client_id: Optional[str] = None,
        timeout: int = 60,
        auth_token: Optional[str] = None
    ):
        """
        Initialize a proxy connector to Blender.
        
        Args:
            proxy_url (str): The URL of the Blender proxy service
            client_id (str, optional): Client ID for authentication
            timeout (int): Timeout for requests in seconds
            auth_token (str, optional): Authentication token
        """
        self.proxy_url = proxy_url.rstrip('/')
        self.client_id = client_id or str(uuid.uuid4())
        self.timeout = timeout
        self.auth_token = auth_token
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'BlenderMCP-ProxyConnector/1.0'
        })
        self.connected = False
        self.last_ping_time = None
        self.ping_interval = 30  # seconds
    
    def connect(self) -> bool:
        """
        Connect to the Blender proxy.
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        if self.connected:
            # Check if we need to ping to keep connection alive
            if self.last_ping_time and (datetime.now() - self.last_ping_time).total_seconds() > self.ping_interval:
                return self._ping()
            return True
            
        try:
            headers = self._get_auth_headers()
            
            # Try to connect to proxy
            response = self.session.post(
                f"{self.proxy_url}/connect",
                json={"client_id": self.client_id},
                headers=headers,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    logger.info(f"Connected to Blender proxy at {self.proxy_url}")
                    self.connected = True
                    self.last_ping_time = datetime.now()
                    
                    # Store any token if provided in response
                    if "token" in data:
                        self.auth_token = data["token"]
                        self.session.headers.update({"Authorization": f"Bearer {self.auth_token}"})
                        
                    return True
                else:
                    logger.error(f"Failed to connect to Blender proxy: {data.get('message', 'Unknown error')}")
            else:
                logger.error(f"Failed to connect to Blender proxy. Status code: {response.status_code}")
                
            return False
        except requests.RequestException as e:
            logger.error(f"Error connecting to Blender proxy: {str(e)}")
            self.connected = False
            return False
    
    def disconnect(self) -> None:
        """
        Disconnect from the Blender proxy.
        """
        if not self.connected:
            return
            
        try:
            headers = self._get_auth_headers()
            
            # Notify proxy we're disconnecting
            response = self.session.post(
                f"{self.proxy_url}/disconnect",
                json={"client_id": self.client_id},
                headers=headers,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                logger.info(f"Disconnected from Blender proxy at {self.proxy_url}")
            else:
                logger.warning(f"Unexpected response when disconnecting from proxy: {response.status_code}")
        except requests.RequestException as e:
            logger.warning(f"Error when disconnecting from Blender proxy: {str(e)}")
        finally:
            self.connected = False
            self.session.close()
    
    def is_connected(self) -> bool:
        """
        Check if the connector is currently connected.
        
        Returns:
            bool: True if connected, False otherwise
        """
        if not self.connected:
            return False
            
        # If it's been a while since our last ping, check connection
        if self.last_ping_time and (datetime.now() - self.last_ping_time).total_seconds() > self.ping_interval:
            return self._ping()
            
        return self.connected
    
    def _ping(self) -> bool:
        """
        Ping the proxy to keep the connection alive and check connectivity.
        
        Returns:
            bool: True if ping was successful, False otherwise
        """
        try:
            headers = self._get_auth_headers()
            
            response = self.session.post(
                f"{self.proxy_url}/ping",
                json={"client_id": self.client_id},
                headers=headers,
                timeout=10  # Use shorter timeout for pings
            )
            
            if response.status_code == 200:
                self.last_ping_time = datetime.now()
                return True
            else:
                logger.warning(f"Ping failed with status code: {response.status_code}")
                self.connected = False
                return False
        except requests.RequestException as e:
            logger.warning(f"Ping failed: {str(e)}")
            self.connected = False
            return False
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """
        Get authentication headers for requests.
        
        Returns:
            Dict[str, str]: Headers dictionary
        """
        headers = {}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        return headers
            
    def send_command(self, command_type: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Send a command to Blender through the proxy and return the response.
        
        Args:
            command_type (str): The type of command to send
            params (Dict[str, Any], optional): Parameters for the command
            
        Returns:
            Dict[str, Any]: The response from Blender
            
        Raises:
            ConnectionError: If unable to connect to Blender proxy
            Exception: For any other errors during command execution
        """
        if not self.connected and not self.connect():
            raise ConnectionError(f"Not connected to Blender proxy at {self.proxy_url}")
        
        command = {
            "type": command_type,
            "params": params or {},
            "client_id": self.client_id,
            "request_id": str(uuid.uuid4())
        }
        
        try:
            # Log the command being sent
            logger.info(f"Sending command via proxy: {command_type} with params: {params}")
            
            headers = self._get_auth_headers()
            
            # Send the command
            response = self.session.post(
                f"{self.proxy_url}/command",
                json=command,
                headers=headers,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                logger.error(f"Error response from proxy: {response.status_code}")
                raise Exception(f"Proxy returned error code: {response.status_code}")
            
            # Process the response
            result = response.json()
            logger.info(f"Received proxy response for command: {command_type}")
            
            # Check if this is a long-running command
            if result.get("status") == "pending":
                # This is a long-running command, poll for the result
                return self._poll_for_result(result.get("task_id"), command_type)
            
            # For immediate responses
            if result.get("status") == "error":
                logger.error(f"Blender error via proxy: {result.get('message')}")
                raise Exception(result.get("message", "Unknown error from Blender"))
            
            return result.get("result", {})
            
        except requests.Timeout:
            logger.error(f"Timeout waiting for proxy response")
            raise Exception(f"Timeout waiting for proxy response - try simplifying your request")
        except requests.RequestException as e:
            logger.error(f"Error communicating with proxy: {str(e)}")
            self.connected = False
            raise Exception(f"Communication error with proxy: {str(e)}")
        except Exception as e:
            logger.error(f"Error during proxy command execution: {str(e)}")
            raise
    
    def _poll_for_result(self, task_id: str, command_type: str) -> Dict[str, Any]:
        """
        Poll for the result of a long-running command.
        
        Args:
            task_id (str): The ID of the task to poll for
            command_type (str): The original command type (for logging)
            
        Returns:
            Dict[str, Any]: The command result once complete
            
        Raises:
            Exception: If polling fails or times out
        """
        logger.info(f"Polling for result of task {task_id} (command: {command_type})")
        
        max_polls = 60  # Maximum number of poll attempts
        poll_interval = 1.0  # Start with 1 second between polls
        max_poll_interval = 5.0  # Maximum interval between polls
        
        for attempt in range(max_polls):
            try:
                headers = self._get_auth_headers()
                
                # Poll for the result
                response = self.session.get(
                    f"{self.proxy_url}/task_result/{task_id}",
                    headers=headers,
                    params={"client_id": self.client_id},
                    timeout=10  # Use shorter timeout for polling
                )
                
                if response.status_code != 200:
                    logger.warning(f"Error polling for task result: {response.status_code}")
                    time.sleep(poll_interval)
                    continue
                
                result = response.json()
                
                # Check the task status
                status = result.get("status")
                
                if status == "completed":
                    logger.info(f"Task {task_id} completed successfully")
                    return result.get("result", {})
                elif status == "error":
                    logger.error(f"Task {task_id} failed: {result.get('message')}")
                    raise Exception(result.get("message", "Unknown error during task execution"))
                elif status == "pending":
                    # Task still running, continue polling
                    logger.debug(f"Task {task_id} still running, polling again in {poll_interval} seconds")
                    time.sleep(poll_interval)
                    
                    # Increase poll interval with exponential backoff (capped)
                    poll_interval = min(poll_interval * 1.5, max_poll_interval)
                else:
                    logger.error(f"Unknown task status: {status}")
                    raise Exception(f"Unknown task status: {status}")
                    
            except requests.RequestException as e:
                logger.warning(f"Error during polling: {str(e)}")
                time.sleep(poll_interval)
                
                # Increase poll interval with exponential backoff (capped)
                poll_interval = min(poll_interval * 1.5, max_poll_interval)
        
        # If we get here, we've exceeded the maximum poll attempts
        logger.error(f"Polling for task {task_id} timed out after {max_polls} attempts")
        raise Exception("Command timed out - the task may still be running on the server")
