"""
Blender MCP Proxy Client

This is a local proxy that connects to a local Blender instance and exposes a 
HTTP API that can be accessed by remote blender-mcp servers.

Usage:
    python -m blender_mcp.proxy_client --host localhost --port 9876 --proxy-port 5000
"""

import os
import sys
import json
import uuid
import time
import socket
import logging
import argparse
import threading
import traceback
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Depends, Header, BackgroundTasks, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("BlenderMCPProxy")

# Command models
class CommandRequest(BaseModel):
    type: str
    params: Dict[str, Any] = {}
    client_id: str
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

class ConnectRequest(BaseModel):
    client_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

class PingRequest(BaseModel):
    client_id: str

class DisconnectRequest(BaseModel):
    client_id: str

class TaskResult(BaseModel):
    status: str  # "pending", "completed", "error"
    task_id: str
    result: Optional[Dict[str, Any]] = None
    message: Optional[str] = None

@dataclass
class BlenderTask:
    task_id: str
    client_id: str
    command_type: str
    params: Dict[str, Any]
    status: str = "pending"
    result: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None

class BlenderProxyClient:
    """
    Proxy client that connects to a local Blender instance and exposes a HTTP API
    """
    
    def __init__(self, blender_host: str = "localhost", blender_port: int = 9876, api_key: Optional[str] = None):
        """
        Initialize the proxy client.
        
        Args:
            blender_host (str): Host where Blender is running
            blender_port (int): Port that Blender is listening on
            api_key (str, optional): API key for authentication
        """
        self.blender_host = blender_host
        self.blender_port = blender_port
        self.api_key = api_key or os.environ.get("BLENDER_PROXY_API_KEY", "")
        
        # Client connections
        self.clients: Dict[str, Dict[str, Any]] = {}
        self.clients_lock = threading.Lock()
        
        # Blender socket
        self.sock = None
        self.connection_lock = threading.Lock()
        
        # Task management
        self.tasks: Dict[str, BlenderTask] = {}
        self.tasks_lock = threading.Lock()
        
        # Task worker thread
        self.task_thread = None
        self.running = False
        
        # Create FastAPI app
        self.app = FastAPI(
            title="Blender MCP Proxy",
            description="Local proxy for Blender MCP communication",
            version="1.0.0"
        )
        self.setup_routes()
        
    def setup_routes(self):
        """Set up API routes"""
        
        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Authentication dependency
        api_key_header = APIKeyHeader(name="Authorization", auto_error=False)
        
        def get_api_key(api_key: str = Depends(api_key_header)):
            if self.api_key and (not api_key or not api_key.startswith("Bearer ") or api_key.replace("Bearer ", "") != self.api_key):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or missing API key",
                )
            return api_key
        
        # Connection routes
        @self.app.post("/connect")
        async def connect(request: ConnectRequest, api_key: str = Depends(get_api_key)):
            """Connect to the proxy"""
            return self.handle_connect(request)
        
        @self.app.post("/disconnect")
        async def disconnect(request: DisconnectRequest, api_key: str = Depends(get_api_key)):
            """Disconnect from the proxy"""
            return self.handle_disconnect(request)
        
        @self.app.post("/ping")
        async def ping(request: PingRequest, api_key: str = Depends(get_api_key)):
            """Ping the proxy to keep connection alive"""
            return self.handle_ping(request)
        
        # Command route
        @self.app.post("/command")
        async def command(
            request: CommandRequest, 
            background_tasks: BackgroundTasks,
            api_key: str = Depends(get_api_key)
        ):
            """Send a command to Blender"""
            return self.handle_command(request, background_tasks)
        
        # Task result route
        @self.app.get("/task_result/{task_id}")
        async def task_result(task_id: str, client_id: str, api_key: str = Depends(get_api_key)):
            """Get the result of a task"""
            return self.handle_task_result(task_id, client_id)
        
        # Health check route (no auth required)
        @self.app.get("/health")
        async def health():
            """Health check endpoint"""
            return {
                "status": "healthy",
                "blender_connected": self.is_blender_connected(),
                "clients": len(self.clients),
                "tasks": {
                    "pending": sum(1 for t in self.tasks.values() if t.status == "pending"),
                    "completed": sum(1 for t in self.tasks.values() if t.status == "completed"),
                    "error": sum(1 for t in self.tasks.values() if t.status == "error")
                }
            }
    
    def start(self, host: str = "127.0.0.1", port: int = 5000):
        """
        Start the proxy server
        
        Args:
            host (str): Host to bind the server to
            port (int): Port to listen on
        """
        # Start task worker thread
        self.running = True
        self.task_thread = threading.Thread(target=self.task_worker)
        self.task_thread.daemon = True
        self.task_thread.start()
        
        # Start FastAPI server
        uvicorn.run(self.app, host=host, port=port)
        
        # Stop task worker when server stops
        self.running = False
        if self.task_thread:
            self.task_thread.join(timeout=5)
    
    def connect_to_blender(self) -> bool:
        """
        Connect to the Blender instance
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        with self.connection_lock:
            if self.sock:
                return True
                
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.blender_host, self.blender_port))
                logger.info(f"Connected to Blender at {self.blender_host}:{self.blender_port}")
                return True
            except Exception as e:
                logger.error(f"Failed to connect to Blender: {str(e)}")
                self.sock = None
                return False
    
    def disconnect_from_blender(self):
        """Disconnect from the Blender instance"""
        with self.connection_lock:
            if self.sock:
                try:
                    self.sock.close()
                except Exception as e:
                    logger.error(f"Error disconnecting from Blender: {str(e)}")
                finally:
                    self.sock = None
    
    def is_blender_connected(self) -> bool:
        """Check if connected to Blender"""
        return self.sock is not None
    
    def send_command_to_blender(self, command_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send a command to Blender and return the response
        
        Args:
            command_type (str): The type of command to send
            params (Dict[str, Any]): Parameters for the command
            
        Returns:
            Dict[str, Any]: The response from Blender
            
        Raises:
            ConnectionError: If unable to connect to Blender
            Exception: For any other errors during command execution
        """
        with self.connection_lock:
            if not self.sock and not self.connect_to_blender():
                raise ConnectionError(f"Not connected to Blender at {self.blender_host}:{self.blender_port}")
            
            command = {
                "type": command_type,
                "params": params or {}
            }
            
            try:
                # Send the command
                self.sock.sendall(json.dumps(command).encode('utf-8'))
                
                # Set a timeout for receiving
                self.sock.settimeout(60.0)  # Longer timeout for potentially slow operations
                
                # Receive the response
                response_data = self.receive_full_response(self.sock)
                
                response = json.loads(response_data.decode('utf-8'))
                
                if response.get("status") == "error":
                    raise Exception(response.get("message", "Unknown error from Blender"))
                
                return response.get("result", {})
            except socket.timeout:
                logger.error("Socket timeout while waiting for response from Blender")
                self.sock = None
                raise Exception("Timeout waiting for Blender response")
            except Exception as e:
                logger.error(f"Error communicating with Blender: {str(e)}")
                self.sock = None
                raise
    
    def receive_full_response(self, sock, buffer_size=8192):
        """
        Receive the complete response, potentially in multiple chunks
        
        Args:
            sock (socket.socket): The socket to receive from
            buffer_size (int): Buffer size for receiving
            
        Returns:
            bytes: The complete response
            
        Raises:
            Exception: If no data is received or the response is incomplete
        """
        chunks = []
        
        try:
            while True:
                try:
                    chunk = sock.recv(buffer_size)
                    if not chunk:
                        if not chunks:
                            raise Exception("Connection closed before receiving any data")
                        break
                    
                    chunks.append(chunk)
                    
                    # Check if we've received a complete JSON object
                    try:
                        data = b''.join(chunks)
                        json.loads(data.decode('utf-8'))
                        # If we get here, it parsed successfully
                        return data
                    except json.JSONDecodeError:
                        # Incomplete JSON, continue receiving
                        continue
                except socket.timeout:
                    # If we hit a timeout during receiving, break the loop and try to use what we have
                    break
        except Exception as e:
            logger.error(f"Error during receive: {str(e)}")
            raise
            
        # If we get here, try to use what we have
        if chunks:
            data = b''.join(chunks)
            try:
                # Try to parse what we have
                json.loads(data.decode('utf-8'))
                return data
            except json.JSONDecodeError:
                # If we can't parse it, it's incomplete
                raise Exception("Incomplete JSON response received")
        else:
            raise Exception("No data received")
    
    def handle_connect(self, request: ConnectRequest) -> Dict[str, Any]:
        """
        Handle a connect request
        
        Args:
            request (ConnectRequest): The connect request
            
        Returns:
            Dict[str, Any]: Response with connection status
        """
        client_id = request.client_id
        
        with self.clients_lock:
            # Check if client already exists
            if client_id in self.clients:
                self.clients[client_id]["last_seen"] = datetime.now()
                logger.info(f"Client {client_id} reconnected")
            else:
                # Add new client
                self.clients[client_id] = {
                    "id": client_id,
                    "connected_at": datetime.now(),
                    "last_seen": datetime.now(),
                    "commands_sent": 0
                }
                logger.info(f"New client connected: {client_id}")
        
        # Try to connect to Blender if not already connected
        blender_connected = self.is_blender_connected() or self.connect_to_blender()
        
        return {
            "status": "success" if blender_connected else "warning",
            "message": "Connected to proxy" + (" and Blender" if blender_connected else ""),
            "blender_connected": blender_connected,
            "client_id": client_id
        }
    
    def handle_disconnect(self, request: DisconnectRequest) -> Dict[str, Any]:
        """
        Handle a disconnect request
        
        Args:
            request (DisconnectRequest): The disconnect request
            
        Returns:
            Dict[str, Any]: Response with disconnection status
        """
        client_id = request.client_id
        
        with self.clients_lock:
            if client_id in self.clients:
                del self.clients[client_id]
                logger.info(f"Client disconnected: {client_id}")
            
        # If no more clients, disconnect from Blender
        with self.clients_lock:
            if not self.clients:
                self.disconnect_from_blender()
                logger.info("No more clients, disconnected from Blender")
        
        return {
            "status": "success",
            "message": "Disconnected from proxy"
        }
    
    def handle_ping(self, request: PingRequest) -> Dict[str, Any]:
        """
        Handle a ping request
        
        Args:
            request (PingRequest): The ping request
            
        Returns:
            Dict[str, Any]: Response with ping status
        """
        client_id = request.client_id
        
        with self.clients_lock:
            if client_id in self.clients:
                self.clients[client_id]["last_seen"] = datetime.now()
                logger.debug(f"Ping from client: {client_id}")
            else:
                logger.warning(f"Ping from unknown client: {client_id}")
                return {
                    "status": "error",
                    "message": "Unknown client"
                }
        
        blender_connected = self.is_blender_connected()
        
        return {
            "status": "success",
            "blender_connected": blender_connected,
            "timestamp": datetime.now().isoformat()
        }
    
    def handle_command(self, request: CommandRequest, background_tasks: BackgroundTasks) -> Dict[str, Any]:
        """
        Handle a command request
        
        Args:
            request (CommandRequest): The command request
            background_tasks (BackgroundTasks): FastAPI background tasks
            
        Returns:
            Dict[str, Any]: Response with command status
        """
        client_id = request.client_id
        command_type = request.type
        params = request.params
        request_id = request.request_id
        
        # Check if client exists
        with self.clients_lock:
            if client_id not in self.clients:
                logger.warning(f"Command from unknown client: {client_id}")
                raise HTTPException(status_code=404, detail="Unknown client")
            
            # Update client stats
            self.clients[client_id]["last_seen"] = datetime.now()
            self.clients[client_id]["commands_sent"] += 1
        
        # Generate task ID
        task_id = str(uuid.uuid4())
        
        # Create task
        task = BlenderTask(
            task_id=task_id,
            client_id=client_id,
            command_type=command_type,
            params=params
        )
        
        # Store task
        with self.tasks_lock:
            self.tasks[task_id] = task
        
        # Determine if command should be executed immediately or in background
        if command_type in ["get_scene_info", "get_object_info", "get_polyhaven_status", "get_hyper3d_status"]:
            # Execute short-running commands immediately
            try:
                result = self.send_command_to_blender(command_type, params)
                
                # Update task with result
                with self.tasks_lock:
                    task.status = "completed"
                    task.result = result
                    task.completed_at = datetime.now()
                
                return {
                    "status": "success",
                    "result": result
                }
            except Exception as e:
                logger.error(f"Error executing command {command_type}: {str(e)}")
                
                # Update task with error
                with self.tasks_lock:
                    task.status = "error"
                    task.error = str(e)
                    task.completed_at = datetime.now()
                
                return {
                    "status": "error",
                    "message": str(e)
                }
        else:
            # Schedule long-running commands to be executed in background
            logger.info(f"Scheduling task {task_id} for background execution: {command_type}")
            
            return {
                "status": "pending",
                "message": f"Command {command_type} scheduled for execution",
                "task_id": task_id
            }
    
    def handle_task_result(self, task_id: str, client_id: str) -> Dict[str, Any]:
        """
        Handle a task result request
        
        Args:
            task_id (str): The task ID
            client_id (str): The client ID
            
        Returns:
            Dict[str, Any]: Response with task result
        """
        # Check if client exists
        with self.clients_lock:
            if client_id not in self.clients:
                logger.warning(f"Task result request from unknown client: {client_id}")
                raise HTTPException(status_code=404, detail="Unknown client")
            
            # Update client last seen
            self.clients[client_id]["last_seen"] = datetime.now()
        
        # Check if task exists
        with self.tasks_lock:
            if task_id not in self.tasks:
                logger.warning(f"Task result request for unknown task: {task_id}")
                raise HTTPException(status_code=404, detail="Unknown task")
            
            task = self.tasks[task_id]
            
            # Check if task belongs to client
            if task.client_id != client_id:
                logger.warning(f"Client {client_id} tried to access task {task_id} belonging to {task.client_id}")
                raise HTTPException(status_code=403, detail="Task does not belong to client")
            
            # Return task result based on status
            if task.status == "completed":
                return {
                    "status": "completed",
                    "result": task.result
                }
            elif task.status == "error":
                return {
                    "status": "error",
                    "message": task.error
                }
            else:
                return {
                    "status": "pending",
                    "message": f"Task {task_id} is still pending"
                }
    
    def task_worker(self):
        """Background worker thread to process tasks"""
        logger.info("Task worker thread started")
        
        while self.running:
            try:
                # Find a pending task
                pending_task = None
                
                with self.tasks_lock:
                    for task in self.tasks.values():
                        if task.status == "pending":
                            pending_task = task
                            # Mark as in-progress to prevent other threads from picking it up
                            task.status = "in-progress"
                            break
                
                if pending_task:
                    logger.info(f"Processing task {pending_task.task_id}: {pending_task.command_type}")
                    
                    try:
                        # Execute the command
                        result = self.send_command_to_blender(pending_task.command_type, pending_task.params)
                        
                        # Update task with result
                        with self.tasks_lock:
                            if pending_task.task_id in self.tasks:  # Check if task still exists
                                pending_task.status = "completed"
                                pending_task.result = result
                                pending_task.completed_at = datetime.now()
                                logger.info(f"Task {pending_task.task_id} completed successfully")
                    except Exception as e:
                        logger.error(f"Error executing task {pending_task.task_id}: {str(e)}")
                        logger.error(traceback.format_exc())
                        
                        # Update task with error
                        with self.tasks_lock:
                            if pending_task.task_id in self.tasks:  # Check if task still exists
                                pending_task.status = "error"
                                pending_task.error = str(e)
                                pending_task.completed_at = datetime.now()
                
                # Clean up old tasks
                self.cleanup_old_tasks()
                
                # No pending tasks, sleep for a bit
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in task worker: {str(e)}")
                logger.error(traceback.format_exc())
                time.sleep(1)  # Sleep longer on error
    
    def cleanup_old_tasks(self):
        """Clean up completed or error tasks older than 30 minutes"""
        with self.tasks_lock:
            now = datetime.now()
            task_ids_to_remove = []
            
            for task_id, task in self.tasks.items():
                if task.status in ["completed", "error"] and task.completed_at:
                    # If task is completed or error and older than 30 minutes, remove it
                    if now - task.completed_at > timedelta(minutes=30):
                        task_ids_to_remove.append(task_id)
            
            for task_id in task_ids_to_remove:
                del self.tasks[task_id]
                
            if task_ids_to_remove:
                logger.info(f"Cleaned up {len(task_ids_to_remove)} old tasks")

def main():
    """Main entry point for the proxy client"""
    parser = argparse.ArgumentParser(description='Blender MCP Proxy Client')
    parser.add_argument('--host', type=str, default='localhost',
                        help='Host where Blender is running (default: localhost)')
    parser.add_argument('--port', type=int, default=9876,
                        help='Port that Blender is listening on (default: 9876)')
    parser.add_argument('--proxy-host', type=str, default='127.0.0.1',
                        help='Host to bind the proxy server to (default: 127.0.0.1)')
    parser.add_argument('--proxy-port', type=int, default=5000,
                        help='Port for the proxy server to listen on (default: 5000)')
    parser.add_argument('--api-key', type=str, default=None,
                        help='API key for authentication (optional)')
    
    args = parser.parse_args()
    
    logger.info(f"Starting Blender MCP Proxy Client, connecting to Blender at {args.host}:{args.port}")
    logger.info(f"Proxy server will listen on {args.proxy_host}:{args.proxy_port}")
    
    # Create proxy client
    proxy = BlenderProxyClient(
        blender_host=args.host,
        blender_port=args.port,
        api_key=args.api_key
    )
    
    # Start proxy server
    proxy.start(host=args.proxy_host, port=args.proxy_port)

if __name__ == "__main__":
    main()
