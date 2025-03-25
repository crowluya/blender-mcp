"""
Base connector for Blender MCP.
Defines the interface that all connectors must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class BlenderConnector(ABC):
    """
    Abstract base class for Blender connectors.
    All connection strategies must implement this interface.
    """
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish a connection to Blender.
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """
        Disconnect from Blender.
        """
        pass
    
    @abstractmethod
    def send_command(self, command_type: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Send a command to Blender and return the response.
        
        Args:
            command_type (str): The type of command to send
            params (Dict[str, Any], optional): Parameters for the command
            
        Returns:
            Dict[str, Any]: The response from Blender
            
        Raises:
            ConnectionError: If unable to connect to Blender
            Exception: For any other errors during command execution
        """
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if the connector is currently connected.
        
        Returns:
            bool: True if connected, False otherwise
        """
        pass
