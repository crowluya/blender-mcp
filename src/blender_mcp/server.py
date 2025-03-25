# blender_mcp_server.py
from mcp.server.fastmcp import FastMCP, Context, Image
import socket
import json
import asyncio
import logging
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Any, List, Optional
import os
from pathlib import Path
import base64
from urllib.parse import urlparse
import time
import uuid
import ssl
import secrets
import argparse
import threading
import tempfile

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BlenderMCPServer")

# 默认 API 密钥长度
API_KEY_LENGTH = 32
# API 密钥验证映射，在生产环境中应该使用数据库存储
API_KEY_TO_USER_ID = {}

@dataclass
class UserSession:
    """用户会话数据类，存储用户的连接信息和实例"""
    user_id: str
    api_key: str
    blender_connections: Dict[str, 'BlenderConnection'] = field(default_factory=dict)
    last_activity: float = field(default_factory=time.time)
    
    def update_activity(self):
        """更新最后活动时间"""
        self.last_activity = time.time()

# 存储用户会话
user_sessions: Dict[str, UserSession] = {}

def validate_api_key(api_key: str) -> Optional[str]:
    """验证 API 密钥并返回用户 ID"""
    # 在生产环境中，应该查询数据库验证 API 密钥
    return API_KEY_TO_USER_ID.get(api_key)

def get_user_session(user_id: str, api_key: str) -> UserSession:
    """获取或创建用户会话"""
    if user_id not in user_sessions:
        user_sessions[user_id] = UserSession(user_id=user_id, api_key=api_key)
    return user_sessions[user_id]

def generate_api_key() -> str:
    """生成一个随机的 API 密钥"""
    return secrets.token_hex(API_KEY_LENGTH // 2)  # 每个字节生成两个十六进制字符

@dataclass
class BlenderConnection:
    host: str
    port: int
    user_id: str = None  # 添加用户 ID
    instance_id: str = None  # 添加实例 ID
    instance_name: str = None  # 添加实例名称
    sock: socket.socket = None  # Changed from 'socket' to 'sock' to avoid naming conflict
    is_remote: bool = False  # 标识是本地还是远程连接
    
    def connect(self) -> bool:
        """Connect to the Blender addon socket server"""
        if self.sock:
            return True
            
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            # 如果是远程连接，使用 SSL 包装 socket
            if self.is_remote:
                try:
                    context = ssl.create_default_context()
                    self.sock = context.wrap_socket(self.sock, server_hostname=self.host)
                    logger.info(f"Using SSL for connection to {self.host}:{self.port}")
                except Exception as e:
                    logger.error(f"Failed to setup SSL: {str(e)}")
                    # 如果 SSL 失败，回退到非加密连接
                    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            self.sock.connect((self.host, self.port))
            
            # 如果有用户 ID 和实例 ID，发送认证信息
            if self.is_remote and self.user_id and self.instance_id:
                auth_data = {
                    "user_id": self.user_id,
                    "instance_id": self.instance_id,
                    "instance_name": self.instance_name or f"Blender-{self.instance_id[:8]}"
                }
                auth_command = {
                    "type": "authenticate",
                    "params": auth_data
                }
                self.sock.sendall(json.dumps(auth_command).encode('utf-8'))
                
                # 等待认证响应
                try:
                    auth_response_data = self.receive_full_response(self.sock)
                    auth_response = json.loads(auth_response_data.decode('utf-8'))
                    
                    if auth_response.get("status") != "success":
                        logger.error(f"Authentication failed: {auth_response.get('message')}")
                        self.sock = None
                        return False
                    logger.info(f"Authentication successful for user {self.user_id}, instance {self.instance_id}")
                except Exception as e:
                    logger.error(f"Error during authentication: {str(e)}")
                    self.sock = None
                    return False
            
            logger.info(f"Connected to Blender at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Blender: {str(e)}")
            self.sock = None
            return False
    
    def disconnect(self):
        """Disconnect from the Blender addon"""
        if self.sock:
            try:
                self.sock.close()
            except Exception as e:
                logger.error(f"Error disconnecting from Blender: {str(e)}")
            finally:
                self.sock = None

    def receive_full_response(self, sock, buffer_size=8192):
        """Receive the complete response, potentially in multiple chunks"""
        chunks = []
        # Use a consistent timeout value that matches the addon's timeout
        sock.settimeout(15.0)  # Match the addon's timeout
        
        try:
            while True:
                try:
                    chunk = sock.recv(buffer_size)
                    if not chunk:
                        # If we get an empty chunk, the connection might be closed
                        if not chunks:  # If we haven't received anything yet, this is an error
                            raise Exception("Connection closed before receiving any data")
                        break
                    
                    chunks.append(chunk)
                    
                    # Check if we've received a complete JSON object
                    try:
                        data = b''.join(chunks)
                        json.loads(data.decode('utf-8'))
                        # If we get here, it parsed successfully
                        logger.info(f"Received complete response ({len(data)} bytes)")
                        return data
                    except json.JSONDecodeError:
                        # Incomplete JSON, continue receiving
                        continue
                except socket.timeout:
                    # If we hit a timeout during receiving, break the loop and try to use what we have
                    logger.warning("Socket timeout during chunked receive")
                    break
                except (ConnectionError, BrokenPipeError, ConnectionResetError) as e:
                    logger.error(f"Socket connection error during receive: {str(e)}")
                    raise  # Re-raise to be handled by the caller
        except socket.timeout:
            logger.warning("Socket timeout during chunked receive")
        except Exception as e:
            logger.error(f"Error during receive: {str(e)}")
            raise
            
        # If we get here, we either timed out or broke out of the loop
        # Try to use what we have
        if chunks:
            data = b''.join(chunks)
            logger.info(f"Returning data after receive completion ({len(data)} bytes)")
            try:
                # Try to parse what we have
                json.loads(data.decode('utf-8'))
                return data
            except json.JSONDecodeError:
                # If we can't parse it, it's incomplete
                raise Exception("Incomplete JSON response received")
        else:
            raise Exception("No data received")

    def send_command(self, command_type: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Send a command to Blender and return the response"""
        if not self.sock and not self.connect():
            raise ConnectionError("Not connected to Blender")
        
        command = {
            "type": command_type,
            "params": params or {}
        }
        
        # 如果有用户 ID 和实例 ID，添加到命令中
        if self.user_id and self.instance_id:
            command["user_id"] = self.user_id
            command["instance_id"] = self.instance_id
        
        try:
            # Log the command being sent
            logger.info(f"Sending command: {command_type} with params: {params}")
            
            # Send the command
            self.sock.sendall(json.dumps(command).encode('utf-8'))
            logger.info(f"Command sent, waiting for response...")
            
            # Set a timeout for receiving - use the same timeout as in receive_full_response
            self.sock.settimeout(15.0)  # Match the addon's timeout
            
            # Receive the response using the improved receive_full_response method
            response_data = self.receive_full_response(self.sock)
            logger.info(f"Received {len(response_data)} bytes of data")
            
            response = json.loads(response_data.decode('utf-8'))
            logger.info(f"Response parsed, status: {response.get('status', 'unknown')}")
            
            if response.get("status") == "error":
                logger.error(f"Blender error: {response.get('message')}")
                raise Exception(response.get("message", "Unknown error from Blender"))
            
            return response.get("result", {})
        except socket.timeout:
            logger.error("Socket timeout while waiting for response from Blender")
            # Don't try to reconnect here - let the get_blender_connection handle reconnection
            # Just invalidate the current socket so it will be recreated next time
            self.sock = None
            raise Exception("Timeout waiting for Blender response - try simplifying your request")
        except (ConnectionError, BrokenPipeError, ConnectionResetError) as e:
            logger.error(f"Socket connection error: {str(e)}")
            self.sock = None
            raise Exception(f"Connection to Blender lost: {str(e)}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from Blender: {str(e)}")
            # Try to log what was received
            if 'response_data' in locals() and response_data:
                logger.error(f"Raw response (first 200 bytes): {response_data[:200]}")
            raise Exception(f"Invalid response from Blender: {str(e)}")
        except Exception as e:
            logger.error(f"Error communicating with Blender: {str(e)}")
            # Don't try to reconnect here - let the get_blender_connection handle reconnection
            self.sock = None
            raise Exception(f"Communication error with Blender: {str(e)}")

@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[Dict[str, Any]]:
    """Manage server startup and shutdown lifecycle"""
    # We don't need to create a connection here since we're using the global connection
    # for resources and tools
    
    try:
        # Just log that we're starting up
        logger.info("BlenderMCP server starting up")
        
        # Try to connect to Blender on startup to verify it's available
        try:
            # This will initialize the global connection if needed
            blender = get_blender_connection()
            logger.info("Successfully connected to Blender on startup")
        except Exception as e:
            logger.warning(f"Could not connect to Blender on startup: {str(e)}")
            logger.warning("Make sure the Blender addon is running before using Blender resources or tools")
        
        # Return an empty context - we're using the global connection
        yield {}
    finally:
        # Clean up the global connection on shutdown
        global _blender_connection
        if _blender_connection:
            logger.info("Disconnecting from Blender on shutdown")
            _blender_connection.disconnect()
            _blender_connection = None
        logger.info("BlenderMCP server shut down")

# Create the MCP server with lifespan support
mcp = FastMCP(
    "BlenderMCP",
    description="Blender integration through the Model Context Protocol",
    lifespan=server_lifespan
)

# Resource endpoints

# Global connection for resources (since resources can't access context)
_blender_connection = None
_polyhaven_enabled = False  # Add this global variable

def get_blender_connection(user_id=None, instance_id=None, is_remote=False, remote_host=None, remote_port=None):
    """Get or create a persistent Blender connection
    
    如果提供了 user_id 和 instance_id，则尝试返回指定用户和实例的连接。
    如果是远程连接，则需要提供 remote_host 和 remote_port。
    如果不提供任何参数，则返回默认的本地连接。
    """
    global _blender_connection, _polyhaven_enabled  # Add _polyhaven_enabled to globals
    
    # 如果指定了用户 ID 和实例 ID，尝试从用户会话中获取连接
    if user_id and instance_id:
        if user_id in user_sessions:
            session = user_sessions[user_id]
            session.update_activity()
            
            # 如果实例连接存在，检查是否有效
            if instance_id in session.blender_connections:
                connection = session.blender_connections[instance_id]
                try:
                    # 发送一个简单的命令检查连接是否有效
                    if is_remote:
                        # 对于远程连接，我们只检查 socket 是否存在
                        if connection.sock:
                            return connection
                    else:
                        # 对于本地连接，我们尝试发送命令
                        result = connection.send_command("get_polyhaven_status")
                        _polyhaven_enabled = result.get("enabled", False)
                        return connection
                except Exception as e:
                    logger.warning(f"Connection for user {user_id}, instance {instance_id} is no longer valid: {str(e)}")
                    try:
                        connection.disconnect()
                    except:
                        pass
                    # 移除无效连接
                    session.blender_connections.pop(instance_id, None)
            
            # 如果到这里，说明需要创建新连接
            if is_remote and remote_host and remote_port:
                connection = BlenderConnection(
                    host=remote_host,
                    port=remote_port,
                    user_id=user_id,
                    instance_id=instance_id,
                    is_remote=True
                )
                if connection.connect():
                    session.blender_connections[instance_id] = connection
                    logger.info(f"Created new remote connection for user {user_id}, instance {instance_id}")
                    return connection
                else:
                    logger.error(f"Failed to create remote connection for user {user_id}, instance {instance_id}")
                    raise Exception("Could not connect to remote Blender instance")
    
    # 下面是处理默认本地连接的原有逻辑
    # If we have an existing connection, check if it's still valid
    if _blender_connection is not None:
        try:
            # First check if PolyHaven is enabled by sending a ping command
            result = _blender_connection.send_command("get_polyhaven_status")
            # Store the PolyHaven status globally
            _polyhaven_enabled = result.get("enabled", False)
            return _blender_connection
        except Exception as e:
            # Connection is dead, close it and create a new one
            logger.warning(f"Existing connection is no longer valid: {str(e)}")
            try:
                _blender_connection.disconnect()
            except:
                pass
            _blender_connection = None
    
    # Create a new connection if needed
    if _blender_connection is None:
        _blender_connection = BlenderConnection(host="localhost", port=9876)
        if not _blender_connection.connect():
            logger.error("Failed to connect to Blender")
            _blender_connection = None
            raise Exception("Could not connect to Blender. Make sure the Blender addon is running.")
        logger.info("Created new persistent connection to Blender")
    
    return _blender_connection

@mcp.tool()
def get_scene_info(ctx: Context) -> str:
    """Get detailed information about the current Blender scene"""
    try:
        blender = get_blender_connection()
        result = blender.send_command("get_scene_info")
        
        # Just return the JSON representation of what Blender sent us
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting scene info from Blender: {str(e)}")
        return f"Error getting scene info: {str(e)}"

@mcp.tool()
def get_object_info(ctx: Context, object_name: str) -> str:
    """
    Get detailed information about a specific object in the Blender scene.
    
    Parameters:
    - object_name: The name of the object to get information about
    """
    try:
        blender = get_blender_connection()
        result = blender.send_command("get_object_info", {"name": object_name})
        
        # Just return the JSON representation of what Blender sent us
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting object info from Blender: {str(e)}")
        return f"Error getting object info: {str(e)}"

@mcp.tool()
def create_object(
    ctx: Context,
    type: str = "CUBE",
    name: str = None,
    location: List[float] = None,
    rotation: List[float] = None,
    scale: List[float] = None,
    # Torus-specific parameters
    align: str = "WORLD",
    major_segments: int = 48,
    minor_segments: int = 12,
    mode: str = "MAJOR_MINOR",
    major_radius: float = 1.0,
    minor_radius: float = 0.25,
    abso_major_rad: float = 1.25,
    abso_minor_rad: float = 0.75,
    generate_uvs: bool = True
) -> str:
    """
    Create a new object in the Blender scene.
    
    Parameters:
    - type: Object type (CUBE, SPHERE, CYLINDER, PLANE, CONE, TORUS, EMPTY, CAMERA, LIGHT)
    - name: Optional name for the object
    - location: Optional [x, y, z] location coordinates
    - rotation: Optional [x, y, z] rotation in radians
    - scale: Optional [x, y, z] scale factors
    - Torus-specific parameters (only used when type == "TORUS"):
    - align: How to align the torus ('WORLD', 'VIEW', or 'CURSOR')
    - major_segments: Number of segments for the main ring
    - minor_segments: Number of segments for the cross-section
    - mode: Dimension mode ('MAJOR_MINOR' or 'EXT_INT')
    - major_radius: Radius from the origin to the center of the cross sections
    - minor_radius: Radius of the torus' cross section
    - abso_major_rad: Total exterior radius of the torus
    - abso_minor_rad: Total interior radius of the torus
    - generate_uvs: Whether to generate a default UV map
    
    Returns:
    A message indicating the created object name.
    """
    try:
        # Get the global connection
        blender = get_blender_connection()
        
        # Set default values for missing parameters
        loc = location or [0, 0, 0]
        rot = rotation or [0, 0, 0]
        sc = scale or [1, 1, 1]
        
        params = {
            "type": type,
            "location": loc,
            "rotation": rot,
        }
        
        if name:
            params["name"] = name

        if type == "TORUS":
            # For torus, the scale is not used.
            params.update({
                "align": align,
                "major_segments": major_segments,
                "minor_segments": minor_segments,
                "mode": mode,
                "major_radius": major_radius,
                "minor_radius": minor_radius,
                "abso_major_rad": abso_major_rad,
                "abso_minor_rad": abso_minor_rad,
                "generate_uvs": generate_uvs
            })
            result = blender.send_command("create_object", params)
            return f"Created {type} object: {result['name']}"
        else:
            # For non-torus objects, include scale
            params["scale"] = sc
            result = blender.send_command("create_object", params)
            return f"Created {type} object: {result['name']}"
    except Exception as e:
        logger.error(f"Error creating object: {str(e)}")
        return f"Error creating object: {str(e)}"

@mcp.tool()
def modify_object(
    ctx: Context,
    name: str,
    location: List[float] = None,
    rotation: List[float] = None,
    scale: List[float] = None,
    visible: bool = None
) -> str:
    """
    Modify an existing object in the Blender scene.
    
    Parameters:
    - name: Name of the object to modify
    - location: Optional [x, y, z] location coordinates
    - rotation: Optional [x, y, z] rotation in radians
    - scale: Optional [x, y, z] scale factors
    - visible: Optional boolean to set visibility
    """
    try:
        # Get the global connection
        blender = get_blender_connection()
        
        params = {"name": name}
        
        if location is not None:
            params["location"] = location
        if rotation is not None:
            params["rotation"] = rotation
        if scale is not None:
            params["scale"] = scale
        if visible is not None:
            params["visible"] = visible
            
        result = blender.send_command("modify_object", params)
        return f"Modified object: {result['name']}"
    except Exception as e:
        logger.error(f"Error modifying object: {str(e)}")
        return f"Error modifying object: {str(e)}"

@mcp.tool()
def delete_object(ctx: Context, name: str) -> str:
    """
    Delete an object from the Blender scene.
    
    Parameters:
    - name: Name of the object to delete
    """
    try:
        # Get the global connection
        blender = get_blender_connection()
        
        result = blender.send_command("delete_object", {"name": name})
        return f"Deleted object: {name}"
    except Exception as e:
        logger.error(f"Error deleting object: {str(e)}")
        return f"Error deleting object: {str(e)}"

@mcp.tool()
def set_material(
    ctx: Context,
    object_name: str,
    material_name: str = None,
    color: List[float] = None
) -> str:
    """
    Set or create a material for an object.
    
    Parameters:
    - object_name: Name of the object to apply the material to
    - material_name: Optional name of the material to use or create
    - color: Optional [R, G, B] color values (0.0-1.0)
    """
    try:
        # Get the global connection
        blender = get_blender_connection()
        
        params = {"object_name": object_name}
        
        if material_name:
            params["material_name"] = material_name
        if color:
            params["color"] = color
            
        result = blender.send_command("set_material", params)
        return f"Applied material to {object_name}: {result.get('material_name', 'unknown')}"
    except Exception as e:
        logger.error(f"Error setting material: {str(e)}")
        return f"Error setting material: {str(e)}"

@mcp.tool()
def execute_blender_code(ctx: Context, code: str) -> str:
    """
    Execute arbitrary Python code in Blender.
    
    Parameters:
    - code: The Python code to execute
    """
    try:
        # Get the global connection
        blender = get_blender_connection()
        
        result = blender.send_command("execute_code", {"code": code})
        return f"Code executed successfully: {result.get('result', '')}"
    except Exception as e:
        logger.error(f"Error executing code: {str(e)}")
        return f"Error executing code: {str(e)}"

@mcp.tool()
def get_polyhaven_categories(ctx: Context, asset_type: str = "hdris") -> str:
    """
    Get a list of categories for a specific asset type on Polyhaven.
    
    Parameters:
    - asset_type: The type of asset to get categories for (hdris, textures, models, all)
    """
    try:
        blender = get_blender_connection()
        if not _polyhaven_enabled:
            return "PolyHaven integration is disabled. Select it in the sidebar in BlenderMCP, then run it again."
        result = blender.send_command("get_polyhaven_categories", {"asset_type": asset_type})
        
        if "error" in result:
            return f"Error: {result['error']}"
        
        # Format the categories in a more readable way
        categories = result["categories"]
        formatted_output = f"Categories for {asset_type}:\n\n"
        
        # Sort categories by count (descending)
        sorted_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)
        
        for category, count in sorted_categories:
            formatted_output += f"- {category}: {count} assets\n"
        
        return formatted_output
    except Exception as e:
        logger.error(f"Error getting Polyhaven categories: {str(e)}")
        return f"Error getting Polyhaven categories: {str(e)}"

@mcp.tool()
def search_polyhaven_assets(
    ctx: Context,
    asset_type: str = "all",
    categories: str = None
) -> str:
    """
    Search for assets on Polyhaven with optional filtering.
    
    Parameters:
    - asset_type: Type of assets to search for (hdris, textures, models, all)
    - categories: Optional comma-separated list of categories to filter by
    
    Returns a list of matching assets with basic information.
    """
    try:
        blender = get_blender_connection()
        result = blender.send_command("search_polyhaven_assets", {
            "asset_type": asset_type,
            "categories": categories
        })
        
        if "error" in result:
            return f"Error: {result['error']}"
        
        # Format the assets in a more readable way
        assets = result["assets"]
        total_count = result["total_count"]
        returned_count = result["returned_count"]
        
        formatted_output = f"Found {total_count} assets"
        if categories:
            formatted_output += f" in categories: {categories}"
        formatted_output += f"\nShowing {returned_count} assets:\n\n"
        
        # Sort assets by download count (popularity)
        sorted_assets = sorted(assets.items(), key=lambda x: x[1].get("download_count", 0), reverse=True)
        
        for asset_id, asset_data in sorted_assets:
            formatted_output += f"- {asset_data.get('name', asset_id)} (ID: {asset_id})\n"
            formatted_output += f"  Type: {['HDRI', 'Texture', 'Model'][asset_data.get('type', 0)]}\n"
            formatted_output += f"  Categories: {', '.join(asset_data.get('categories', []))}\n"
            formatted_output += f"  Downloads: {asset_data.get('download_count', 'Unknown')}\n\n"
        
        return formatted_output
    except Exception as e:
        logger.error(f"Error searching Polyhaven assets: {str(e)}")
        return f"Error searching Polyhaven assets: {str(e)}"

@mcp.tool()
def download_polyhaven_asset(
    ctx: Context,
    asset_id: str,
    asset_type: str,
    resolution: str = "1k",
    file_format: str = None
) -> str:
    """
    Download and import a Polyhaven asset into Blender.
    
    Parameters:
    - asset_id: The ID of the asset to download
    - asset_type: The type of asset (hdris, textures, models)
    - resolution: The resolution to download (e.g., 1k, 2k, 4k)
    - file_format: Optional file format (e.g., hdr, exr for HDRIs; jpg, png for textures; gltf, fbx for models)
    
    Returns a message indicating success or failure.
    """
    try:
        blender = get_blender_connection()
        result = blender.send_command("download_polyhaven_asset", {
            "asset_id": asset_id,
            "asset_type": asset_type,
            "resolution": resolution,
            "file_format": file_format
        })
        
        if "error" in result:
            return f"Error: {result['error']}"
        
        if result.get("success"):
            message = result.get("message", "Asset downloaded and imported successfully")
            
            # Add additional information based on asset type
            if asset_type == "hdris":
                return f"{message}. The HDRI has been set as the world environment."
            elif asset_type == "textures":
                material_name = result.get("material", "")
                maps = ", ".join(result.get("maps", []))
                return f"{message}. Created material '{material_name}' with maps: {maps}."
            elif asset_type == "models":
                return f"{message}. The model has been imported into the current scene."
            else:
                return message
        else:
            return f"Failed to download asset: {result.get('message', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error downloading Polyhaven asset: {str(e)}")
        return f"Error downloading Polyhaven asset: {str(e)}"

@mcp.tool()
def set_texture(
    ctx: Context,
    object_name: str,
    texture_id: str
) -> str:
    """
    Apply a previously downloaded Polyhaven texture to an object.
    
    Parameters:
    - object_name: Name of the object to apply the texture to
    - texture_id: ID of the Polyhaven texture to apply (must be downloaded first)
    
    Returns a message indicating success or failure.
    """
    try:
        # Get the global connection
        blender = get_blender_connection()
        
        result = blender.send_command("set_texture", {
            "object_name": object_name,
            "texture_id": texture_id
        })
        
        if "error" in result:
            return f"Error: {result['error']}"
        
        if result.get("success"):
            material_name = result.get("material", "")
            maps = ", ".join(result.get("maps", []))
            
            # Add detailed material info
            material_info = result.get("material_info", {})
            node_count = material_info.get("node_count", 0)
            has_nodes = material_info.get("has_nodes", False)
            texture_nodes = material_info.get("texture_nodes", [])
            
            output = f"Successfully applied texture '{texture_id}' to {object_name}.\n"
            output += f"Using material '{material_name}' with maps: {maps}.\n\n"
            output += f"Material has nodes: {has_nodes}\n"
            output += f"Total node count: {node_count}\n\n"
            
            if texture_nodes:
                output += "Texture nodes:\n"
                for node in texture_nodes:
                    output += f"- {node['name']} using image: {node['image']}\n"
                    if node['connections']:
                        output += "  Connections:\n"
                        for conn in node['connections']:
                            output += f"    {conn}\n"
            else:
                output += "No texture nodes found in the material.\n"
            
            return output
        else:
            return f"Failed to apply texture: {result.get('message', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error applying texture: {str(e)}")
        return f"Error applying texture: {str(e)}"

@mcp.tool()
def get_polyhaven_status(ctx: Context) -> str:
    """
    Check if PolyHaven integration is enabled in Blender.
    Returns a message indicating whether PolyHaven features are available.
    """
    try:
        blender = get_blender_connection()
        result = blender.send_command("get_polyhaven_status")
        enabled = result.get("enabled", False)
        message = result.get("message", "")
        
        return message
    except Exception as e:
        logger.error(f"Error checking PolyHaven status: {str(e)}")
        return f"Error checking PolyHaven status: {str(e)}"

@mcp.tool()
def get_hyper3d_status(ctx: Context) -> str:
    """
    Check if Hyper3D Rodin integration is enabled in Blender.
    Returns a message indicating whether Hyper3D Rodin features are available.

    Don't emphasize the key type in the returned message, but sliently remember it. 
    """
    try:
        blender = get_blender_connection()
        result = blender.send_command("get_hyper3d_status")
        enabled = result.get("enabled", False)
        message = result.get("message", "")
        if enabled:
            message += ""
        return message
    except Exception as e:
        logger.error(f"Error checking Hyper3D status: {str(e)}")
        return f"Error checking Hyper3D status: {str(e)}"

@mcp.tool()
def generate_hyper3d_model_via_text(
    ctx: Context,
    text_prompt: str,
    bbox_condition: list[float]=None
) -> str:
    """
    Generate 3D asset using Hyper3D by giving description of the desired asset, and import the asset into Blender.
    The 3D asset has built-in materials.
    The generated model has a normalized size, so re-scaling after generation can be useful.
    
    Parameters:
    - text_prompt: A short description of the desired model in **English**.
    - bbox_condition: Optional. If given, it has to be a list of floats of length 3. Controls the ratio between [Length, Width, Height] of the model. The final size of the model is normalized.

    Returns a message indicating success or failure.
    """
    try:
        blender = get_blender_connection()
        result = blender.send_command("create_rodin_job", {
            "text_prompt": text_prompt,
            "images": None,
            "bbox_condition": bbox_condition,
        })
        succeed = result.get("submit_time", False)
        if succeed:
            return json.dumps({
                "task_uuid": result["uuid"],
                "subscription_key": result["jobs"]["subscription_key"],
            })
        else:
            return json.dumps(result)
    except Exception as e:
        logger.error(f"Error generating Hyper3D task: {str(e)}")
        return f"Error generating Hyper3D task: {str(e)}"
    return f"Placeholder, under development, not implemented yet."

@mcp.tool()
def generate_hyper3d_model_via_images(
    ctx: Context,
    input_image_paths: list[str]=None,
    input_image_urls: list[str]=None,
    bbox_condition: list[float]=None
) -> str:
    """
    Generate 3D asset using Hyper3D by giving images of the wanted asset, and import the generated asset into Blender.
    The 3D asset has built-in materials.
    The generated model has a normalized size, so re-scaling after generation can be useful.
    
    Parameters:
    - input_image_paths: The **absolute** paths of input images. Even if only one image is provided, wrap it into a list. Required if Hyper3D Rodin in MAIN_SITE mode.
    - input_image_urls: The URLs of input images. Even if only one image is provided, wrap it into a list. Required if Hyper3D Rodin in FAL_AI mode.
    - bbox_condition: Optional. If given, it has to be a list of ints of length 3. Controls the ratio between [Length, Width, Height] of the model. The final size of the model is normalized.

    Only one of {input_image_paths, input_image_urls} should be given at a time, depending on the Hyper3D Rodin's current mode.
    Returns a message indicating success or failure.
    """
    if input_image_paths is not None and input_image_urls is not None:
        return f"Error: Conflict parameters given!"
    if input_image_paths is None and input_image_urls is None:
        return f"Error: No image given!"
    if input_image_paths is not None:
        if not all(os.path.exists(i) for i in input_image_paths):
            return "Error: not all image paths are valid!"
        images = []
        for path in input_image_paths:
            with open(path, "rb") as f:
                images.append(
                    (Path(path).suffix, base64.b64encode(f.read()).decode("ascii"))
                )
    elif input_image_urls is not None:
        if not all(urlparse(i) for i in input_image_paths):
            return "Error: not all image URLs are valid!"
        images = input_image_urls.copy()
    try:
        blender = get_blender_connection()
        result = blender.send_command("create_rodin_job", {
            "text_prompt": None,
            "images": images,
            "bbox_condition": bbox_condition,
        })
        succeed = result.get("submit_time", False)
        if succeed:
            return json.dumps({
                "task_uuid": result["uuid"],
                "subscription_key": result["jobs"]["subscription_key"],
            })
        else:
            return json.dumps(result)
    except Exception as e:
        logger.error(f"Error generating Hyper3D task: {str(e)}")
        return f"Error generating Hyper3D task: {str(e)}"

@mcp.tool()
def poll_rodin_job_status(
    ctx: Context,
    subscription_key: str=None,
    request_id: str=None,
):
    """
    Check if the Hyper3D Rodin generation task is completed.

    For Hyper3D Rodin mode MAIN_SITE:
        Parameters:
        - subscription_key: The subscription_key given in the generate model step.

        Returns a list of status. The task is done if all status are "Done".
        If "Failed" showed up, the generating process failed.
        This is a polling API, so only proceed if the status are finally determined ("Done" or "Canceled").

    For Hyper3D Rodin mode FAL_AI:
        Parameters:
        - request_id: The request_id given in the generate model step.

        Returns the generation task status. The task is done if status is "COMPLETED".
        The task is in progress if status is "IN_PROGRESS".
        If status other than "COMPLETED", "IN_PROGRESS", "IN_QUEUE" showed up, the generating process might be failed.
        This is a polling API, so only proceed if the status are finally determined ("COMPLETED" or some failed state).
    """
    try:
        blender = get_blender_connection()
        kwargs = {}
        if subscription_key:
            kwargs = {
                "subscription_key": subscription_key,
            }
        elif request_id:
            kwargs = {
                "request_id": request_id,
            }
        result = blender.send_command("poll_rodin_job_status", kwargs)
        return result
    except Exception as e:
        logger.error(f"Error generating Hyper3D task: {str(e)}")
        return f"Error generating Hyper3D task: {str(e)}"

@mcp.tool()
def import_generated_asset(
    ctx: Context,
    name: str,
    task_uuid: str=None,
    request_id: str=None,
):
    """
    Import the asset generated by Hyper3D Rodin after the generation task is completed.

    Parameters:
    - name: The name of the object in scene
    - task_uuid: For Hyper3D Rodin mode MAIN_SITE: The task_uuid given in the generate model step.
    - request_id: For Hyper3D Rodin mode FAL_AI: The request_id given in the generate model step.

    Only give one of {task_uuid, request_id} based on the Hyper3D Rodin Mode!
    Return if the asset has been imported successfully.
    """
    try:
        blender = get_blender_connection()
        kwargs = {
            "name": name
        }
        if task_uuid:
            kwargs["task_uuid"] = task_uuid
        elif request_id:
            kwargs["request_id"] = request_id
        result = blender.send_command("import_generated_asset", kwargs)
        return result
    except Exception as e:
        logger.error(f"Error generating Hyper3D task: {str(e)}")
        return f"Error generating Hyper3D task: {str(e)}"

@mcp.prompt()
def asset_creation_strategy() -> str:
    """Defines the preferred strategy for creating assets in Blender"""
    return """When creating 3D content in Blender, always start by checking if integrations are available:

    0. Before anything, always check the scene from get_scene_info()
    1. First use the following tools to verify if the following integrations are enabled:
        1. PolyHaven
            Use get_polyhaven_status() to verify its status
            If PolyHaven is enabled:
            - For objects/models: Use download_polyhaven_asset() with asset_type="models"
            - For materials/textures: Use download_polyhaven_asset() with asset_type="textures"
            - For environment lighting: Use download_polyhaven_asset() with asset_type="hdris"
        2. Hyper3D(Rodin)
            Hyper3D Rodin is good at generating 3D models for single item.
            So don't try to:
            1. Generate the whole scene with one shot
            2. Generate ground using Rodin
            3. Generate parts of the items separately and put them together afterwards

            Use get_hyper3d_status() to verify its status
            If Hyper3D is enabled:
            - For objects/models, do the following steps:
                1. Create the model generation task
                    - Use generate_hyper3d_model_via_images() if image(s) is/are given
                    - Use generate_hyper3d_model_via_text() if generating 3D asset using text prompt
                    If key type is free_trial and insufficient balance error returned, tell the user that the free trial key can only generated limited models everyday, they can choose to:
                    - Wait for another day and try again
                    - Go to hyper3d.ai to find out how to get their own API key
                    - Go to fal.ai to get their own private API key
                2. Poll the status
                    - Use poll_rodin_job_status() to check if the generation task has completed or failed
                3. Import the asset
                    - Use import_generated_asset() to import the generated GLB model the asset
                4. After importing the asset, ALWAYS check the world_bounding_box of the imported mesh, and adjust the mesh's location and size
                    Adjust the imported mesh's location, scale, rotation, so that the mesh is on the right spot.

                You can reuse assets previous generated by running python code to duplicate the object, without creating another generation task.
       
    2. If all integrations are disabled or when falling back to basic tools:
       - create_object() for basic primitives (CUBE, SPHERE, CYLINDER, etc.)
       - set_material() for basic colors and materials
    
    3. When including an object into scene, ALWAYS make sure that the name of the object is meanful.

    4. Always check the world_bounding_box for each item so that:
        - Ensure that all objects that should not be clipping are not clipping.
        - Items have right spatial relationship.
    
    5. After giving the tool location/scale/rotation information (via create_object() and modify_object()),
       double check the related object's location, scale, rotation, and world_bounding_box using get_object_info(),
       so that the object is in the desired location.

    Only fall back to basic creation tools when:
    - PolyHaven and Hyper3D are disabled
    - A simple primitive is explicitly requested
    - No suitable PolyHaven asset exists
    - Hyper3D Rodin failed to generate the desired asset
    - The task specifically requires a basic material/color
    """

# 用户管理和实例注册的工具函数

@mcp.tool()
def create_user(ctx: Context, username: str) -> str:
    """
    创建一个新用户并生成 API 密钥
    
    Parameters:
    - username: 用户名，用于标识用户
    
    Returns:
    包含用户 ID 和 API 密钥的 JSON 字符串
    """
    try:
        # 生成用户 ID 和 API 密钥
        user_id = str(uuid.uuid4())
        api_key = generate_api_key()
        
        # 将 API 密钥与用户 ID 关联
        API_KEY_TO_USER_ID[api_key] = user_id
        
        # 创建用户会话
        get_user_session(user_id, api_key)
        
        # 返回用户信息
        user_info = {
            "user_id": user_id,
            "username": username,
            "api_key": api_key
        }
        logger.info(f"Created new user: {username} (ID: {user_id})")
        
        return json.dumps(user_info)
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        return f"Error creating user: {str(e)}"

@mcp.tool()
def register_blender_instance(ctx: Context, api_key: str, instance_name: str = None) -> str:
    """
    注册一个新的 Blender 实例
    
    Parameters:
    - api_key: 用户的 API 密钥
    - instance_name: 可选的实例名称
    
    Returns:
    包含实例 ID 和连接信息的 JSON 字符串
    """
    try:
        # 验证 API 密钥
        user_id = validate_api_key(api_key)
        if not user_id:
            raise Exception("无效的 API 密钥")
        
        # 生成实例 ID
        instance_id = str(uuid.uuid4())
        
        # 设置实例名称
        if not instance_name:
            instance_name = f"Blender-{instance_id[:8]}"
        
        # 获取用户会话并添加实例信息
        session = get_user_session(user_id, api_key)
        
        # 创建实例信息字典（尚未连接）
        instance_info = {
            "instance_id": instance_id,
            "instance_name": instance_name,
            "user_id": user_id,
            "registered_at": time.time()
        }
        
        # 返回实例信息
        logger.info(f"Registered new Blender instance for user {user_id}: {instance_name} (ID: {instance_id})")
        
        return json.dumps(instance_info)
    except Exception as e:
        logger.error(f"Error registering Blender instance: {str(e)}")
        return f"Error registering Blender instance: {str(e)}"

@mcp.tool()
def list_blender_instances(ctx: Context, api_key: str) -> str:
    """
    列出用户的所有 Blender 实例
    
    Parameters:
    - api_key: 用户的 API 密钥
    
    Returns:
    包含实例列表的 JSON 字符串
    """
    try:
        # 验证 API 密钥
        user_id = validate_api_key(api_key)
        if not user_id:
            raise Exception("无效的 API 密钥")
        
        # 获取用户会话
        if user_id not in user_sessions:
            return json.dumps({"instances": []})
        
        session = user_sessions[user_id]
        
        # 收集所有实例信息
        instances = []
        for instance_id, connection in session.blender_connections.items():
            instances.append({
                "instance_id": instance_id,
                "instance_name": connection.instance_name or f"Blender-{instance_id[:8]}",
                "connected": connection.sock is not None,
                "host": connection.host,
                "port": connection.port
            })
        
        return json.dumps({"instances": instances})
    except Exception as e:
        logger.error(f"Error listing Blender instances: {str(e)}")
        return f"Error listing Blender instances: {str(e)}"

@mcp.tool()
def route_command(ctx: Context, api_key: str, target_instance_id: str, command_type: str, params: Dict[str, Any] = None) -> str:
    """
    将命令路由到指定的 Blender 实例
    
    Parameters:
    - api_key: 用户的 API 密钥
    - target_instance_id: 目标 Blender 实例的 ID
    - command_type: 要发送的命令类型
    - params: 命令参数
    
    Returns:
    目标实例返回的结果
    """
    try:
        # 验证 API 密钥
        user_id = validate_api_key(api_key)
        if not user_id:
            raise Exception("无效的 API 密钥")
        
        # 获取用户会话
        if user_id not in user_sessions:
            raise Exception("用户会话不存在")
        
        session = user_sessions[user_id]
        
        # 检查目标实例是否存在
        if target_instance_id not in session.blender_connections:
            raise Exception(f"目标 Blender 实例 {target_instance_id} 不存在或不属于当前用户")
        
        # 获取目标连接并发送命令
        target_connection = session.blender_connections[target_instance_id]
        result = target_connection.send_command(command_type, params)
        
        return json.dumps(result)
    except Exception as e:
        logger.error(f"Error routing command: {str(e)}")
        return f"Error routing command: {str(e)}"

# Main execution

def main():
    """Run the Blender MCP server"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Blender MCP Server')
    parser.add_argument('--host', default='localhost', help='Host to listen on')
    parser.add_argument('--port', type=int, default=5000, help='Port to listen on')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--allow-remote', action='store_true', help='Allow remote connections')
    parser.add_argument('--ssl-cert', help='Path to SSL certificate file')
    parser.add_argument('--ssl-key', help='Path to SSL key file')
    parser.add_argument('--admin-key', help='API key for admin access')
    
    args = parser.parse_args()
    
    # 设置全局配置
    global ALLOW_REMOTE_CONNECTIONS, ADMIN_API_KEY
    ALLOW_REMOTE_CONNECTIONS = args.allow_remote
    ADMIN_API_KEY = args.admin_key or secrets.token_hex(16)
    
    # 设置会话清理计时器
    session_cleanup_thread = threading.Thread(target=_session_cleanup_loop)
    session_cleanup_thread.daemon = True
    session_cleanup_thread.start()
    
    if args.allow_remote:
        logger.info(f"Server will accept remote connections on {args.host}:{args.port}")
        logger.info(f"Admin API Key: {ADMIN_API_KEY}")
        
        # 如果没有提供 SSL 证书和密钥，但允许远程连接，生成自签名证书
        if not args.ssl_cert or not args.ssl_key:
            logger.warning("No SSL certificate provided for remote connections. Generating self-signed certificate...")
            
            # 在生产环境中，应该使用适当的证书
            cert_file = os.path.join(tempfile.gettempdir(), "blender_mcp_cert.pem")
            key_file = os.path.join(tempfile.gettempdir(), "blender_mcp_key.pem")
            
            # 这里应该有生成自签名证书的代码
            # 例如使用 OpenSSL 或 Python 的 cryptography 库
            
            args.ssl_cert = cert_file
            args.ssl_key = key_file
    else:
        if args.host != 'localhost' and args.host != '127.0.0.1':
            logger.warning("Remote connections are not allowed, forcing host to localhost")
            args.host = 'localhost'
    
    # 创建管理员会话
    admin_user_id = "admin"
    API_KEY_TO_USER_ID[ADMIN_API_KEY] = admin_user_id
    admin_session = UserSession(user_id=admin_user_id, api_key=ADMIN_API_KEY)
    user_sessions[admin_user_id] = admin_session
    
    # 配置 SSL 上下文（如果启用了 SSL）
    ssl_context = None
    if args.ssl_cert and args.ssl_key:
        try:
            import ssl
            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_context.load_cert_chain(args.ssl_cert, args.ssl_key)
            logger.info("SSL enabled for server")
        except Exception as e:
            logger.error(f"Failed to setup SSL: {str(e)}")
            if args.allow_remote:
                logger.error("Remote connections are allowed but SSL failed to initialize. This is insecure!")
    
    # 启动服务器
    mcp_server = mcp.server.fastmcp.FastMCPServer(
        app_dir=os.path.dirname(__file__),
        host=args.host,
        port=args.port,
        debug=args.debug,
        ssl_context=ssl_context
    )
    
    logger.info(f"Starting Blender MCP server on {args.host}:{args.port}")
    mcp_server.serve()

# 会话清理循环
def _session_cleanup_loop():
    """周期性清理不活跃的会话"""
    while True:
        try:
            cleanup_inactive_sessions()
        except Exception as e:
            logger.error(f"Error cleaning up sessions: {str(e)}")
        
        # 每 5 分钟检查一次
        time.sleep(5 * 60)

# 用户会话清理计时器
def cleanup_inactive_sessions():
    """清理不活跃的用户会话"""
    current_time = time.time()
    sessions_to_remove = []
    
    for user_id, session in user_sessions.items():
        # 如果会话超过 30 分钟不活跃，则清理
        if current_time - session.last_activity > 30 * 60:
            logger.info(f"Cleaning up inactive session for user {user_id}")
            # 断开所有连接
            for instance_id, conn in session.blender_connections.items():
                try:
                    conn.disconnect()
                except:
                    pass
            sessions_to_remove.append(user_id)
    
    # 从字典中移除会话
    for user_id in sessions_to_remove:
        user_sessions.pop(user_id, None)

if __name__ == "__main__":
    main()