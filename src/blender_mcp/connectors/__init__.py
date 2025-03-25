"""
Blender MCP connector modules.
This package provides different connection strategies for connecting to Blender.
"""

from .base import BlenderConnector
from .direct import DirectConnector
from .proxy import ProxyConnector

# Factory function to create the appropriate connector
def create_connector(mode="direct", **kwargs):
    """
    Create a connector based on the specified mode.
    
    Args:
        mode (str): Connection mode - 'direct' or 'proxy'
        **kwargs: Additional arguments for the specific connector
        
    Returns:
        BlenderConnector: The appropriate connector instance
    """
    if mode.lower() == "direct":
        # For direct mode, we need host and port
        host = kwargs.get("host", "localhost")
        port = kwargs.get("port", 9876)
        return DirectConnector(host=host, port=port)
    elif mode.lower() == "proxy":
        # For proxy mode, we need the proxy URL and client ID
        proxy_url = kwargs.get("proxy_url", None)
        client_id = kwargs.get("client_id", None)
        
        if not proxy_url:
            raise ValueError("proxy_url is required for proxy mode")
            
        return ProxyConnector(
            proxy_url=proxy_url, 
            client_id=client_id,
            timeout=kwargs.get("timeout", 60)
        )
    else:
        raise ValueError(f"Unknown connection mode: {mode}")
