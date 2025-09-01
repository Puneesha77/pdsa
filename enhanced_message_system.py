"""
WebSocket handler for Enhanced Message System
Integrates with Priority Queue, Batch Queue, and Retry Queue
"""

import asyncio
import json
import websockets
import logging
from typing import Set, Dict, Any
from enhanced_message_system import EnhancedMessageSystem

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MessageSystemWebSocketHandler:
    def __init__(self, host: str = "localhost", port: int = 8000):
        self.host = host
        self.port = port
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        
        # Initialize the enhanced message system
        self.message_system = EnhancedMessageSystem(
            max_history_size=1000,
            min_batch_size=5,
            max_batch_size=50,
            batch_timeout=2.0,
            max_retries=5
        )
        
        # Start background task for broadcasting stats
        self.stats_task = None
        
        print("🚀 WebSocket Handler initialized with Enhanced Message System")

    async def register_client(self, websocket):
        """Register a new WebSocket client"""
        self.clients.add(websocket)
        logger.info(f"Client connected. Total clients: {len(self.clients)}")
        
        # Send initial stats to new client
        await self.send_stats_to_client(websocket)

    async def unregister_client(self, websocket):
        """Unregister a WebSocket client"""
        self.clients.discard(websocket)
        logger.info(f"Client disconnected. Total clients: {len(self.clients)}")

    async def broadcast_to_clients(self, message: Dict[str, Any]):
        """Broadcast a message to all connected clients"""
        if not self.clients:
            return
        
        message_json = json.dumps(message)
        disconnected_clients = set()
        
        for client in self.clients.copy():
            try:
                await client.send(message_json)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error(f"Error sending to client: {e}")
                disconnected_clients.add(client)
        
        # Remove disconnected clients
        for client in disconnected_clients:
            self.clients.discard(client)

    async def send_stats_to_client(self, websocket):
        """Send current stats to a specific client"""
        try:
            stats = self.message_system.get_comprehensive_stats()
            queue_status = self.message_system.get_queue_status()
            
            response = {
                'type': 'stats_update',
                'stats': stats,
                'queue_status': queue_status
            }
            
            await websocket.send(json.dumps(response))
        except Exception as e:
            logger.error(f"Error sending stats to client: {e}")

    async def handle_message(self, websocket, message: str):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)
            action = data.get('action')
            
            if action == 'send_message':
                await self.handle_send_message(websocket, data)
            
            elif action == 'get_comprehensive_stats':
                await self.send_stats_to_client(websocket)
            
            elif action == 'force_send_batch':
                await self.handle_force_send_batch(websocket)
            
            elif action == 'send_test_retry_message':
                await self.handle_send_test_retry_message(websocket)
            
            elif action == 'force_retry_all':
                await self.handle_force_retry_all(websocket)
            
            elif action == 'get_next_message':
                await self.handle_get_next_message(websocket)
            
            elif action == 'clear_all_queues':
                await self.handle_clear_all_queues(websocket)
            
            else:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': f'Unknown action: {action}'
                }))
                
        except json.JSONDecodeError:
            await websocket.send(json.dumps({
                'type': 'error',
                'message': 'Invalid JSON format'
            }))
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            await websocket.send(json.dumps({
                'type': 'error',
                'message': f'Server error: {str(e)}'
            }))

    async def handle_send_message(self, websocket, data):
        """Handle sending a new message through the enhanced system"""
        try:
            message_text = data.get('message', '')
            user = data.get('user', 'Anonymous')
            manual_priority = data.get('priority')
            
            if not message_text.strip():
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': 'Message text cannot be empty'
                }))
                return
            
            # Add message to the enhanced system
            message_obj = self.message_system.add_message(
                message=message_text,
                user=user,
                manual_priority=manual_priority
            )
            
            # Broadcast updated priority queue to all clients
            history = self.message_system.get_history()
            await self.broadcast_to_clients({
                'type': 'message_added',
                'message': message_obj,
                'messages': history[-10:]  # Send last 10 messages
            })
            
            # Send confirmation to sender
            await websocket.send(json.dumps({
                'type': 'message_sent',
                'message': 'Message added to enhanced system'
            }))
            
        except Exception as e:
            logger.error(f"Error handling send message: {e}")
            await websocket.send(json.dumps({
                'type': 'error',
                'message': f'Error sending message: {str(e)}'
            }))

    async def handle_force_send_batch(self, websocket):
        """Handle force sending current batch"""
        try:
            count = self.message_system.force_send_pending_batches()
            
            await self.broadcast_to_clients({
                'type': 'batch_sent',
                'count': count,
                'message': f'Forced batch send: {count} messages'
            })
            
        except Exception as e:
            logger.error(f"Error forcing batch send: {e}")
            await websocket.send(json.dumps({
                'type': 'error',
                'message': f'Error forcing batch send: {str(e)}'
            }))

    async def handle_send_test_retry_message(self, websocket):
        """Send a message that will intentionally fail for retry testing"""
        try:
            # Create a test message that will fail
            test_message = {
                'message': 'This is a test retry message',
                'user': 'TestUser',
                'timestamp': '2024-01-01T00:00:00',
                'priority': 3,
                'test_failure': True  # This flag will cause intentional failure
            }
            
            # Add directly to retry queue to simulate a failed message
            self.message_system.retry_queue.enqueue(
                test_message, 
                "Intentional test failure"
            )
            
            await websocket.send(json.dumps({
                'type': 'test_retry_sent',
                'message': 'Test retry message added to retry queue'
            }))
            
        except Exception as e:
            logger.error(f"Error sending test retry message: {e}")
            await websocket.send(json.dumps({
                'type': 'error',
                'message': f'Error sending test retry: {str(e)}'
            }))

    async def handle_force_retry_all(self, websocket):
        """Handle forcing retry of all failed messages"""
        try:
            count = self.message_system.retry_failed_messages()
            
            await self.broadcast_to_clients({
                'type': 'retry_forced',
                'count': count,
                'message': f'Forced retry: {count} messages'
            })
            
        except Exception as e:
            logger.error(f"Error forcing retries: {e}")
            await websocket.send(json.dumps({
                'type': 'error',
                'message': f'Error forcing retries: {str(e)}'
            }))

    async def handle_get_next_message(self, websocket):
        """Handle getting the next priority message"""
        try:
            message = self.message_system.get_next_message()
            
            await websocket.send(json.dumps({
                'type': 'next_message',
                'message': message
            }))
            
        except Exception as e:
            logger.error(f"Error getting next message: {e}")
            await websocket.send(json.dumps({
                'type': 'error',
                'message': f'Error getting next message: {str(e)}'
            }))

    async def handle_clear_all_queues(self, websocket):
        """Handle clearing all queues"""
        try:
            result = self.message_system.clear_all_queues()
            
            await self.broadcast_to_clients({
                'type': 'queues_cleared',
                'result': result
            })
            
        except Exception as e:
            logger.error(f"Error clearing queues: {e}")
            await websocket.send(json.dumps({
                'type': 'error',
                'message': f'Error clearing queues: {str(e)}'
            }))

    async def stats_broadcaster(self):
        """Background task to broadcast stats periodically"""
        while True:
            try:
                if self.clients:
                    stats = self.message_system.get_comprehensive_stats()
                    queue_status = self.message_system.get_queue_status()
                    
                    await self.broadcast_to_clients({
                        'type': 'stats_update',
                        'stats': stats,
                        'queue_status': queue_status
                    })
                
                await asyncio.sleep(2)  # Broadcast every 2 seconds
                
            except Exception as e:
                logger.error(f"Error in stats broadcaster: {e}")
                await asyncio.sleep(5)  # Wait longer on error

    async def handle_client(self, websocket, path):
        """Handle individual WebSocket client connection"""
        await self.register_client(websocket)
        
        try:
            async for message in websocket:
                await self.handle_message(websocket, message)
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            await self.unregister_client(websocket)

    async def start_server(self):
        """Start the WebSocket server"""
        # Start the stats broadcaster
        self.stats_task = asyncio.create_task(self.stats_broadcaster())
        
        # Start the WebSocket server
        logger.info(f"Starting WebSocket server on {self.host}:{self.port}")
        
        async with websockets.serve(
            self.handle_client,
            self.host,
            self.port,
            ping_interval=30,
            ping_timeout=10
        ):
            logger.info(f"🔄 Enhanced Message System server running on ws://{self.host}:{self.port}")
            await asyncio.Future()  # Run forever

    def stop_server(self):
        """Stop the server gracefully"""
        if self.stats_task:
            self.stats_task.cancel()
        
        self.message_system.shutdown()
        logger.info("🛑 Enhanced Message System server stopped")


# Main execution
async def main():
    """Main function to run the WebSocket server"""
    handler = MessageSystemWebSocketHandler()
    
    try:
        await handler.start_server()
    except KeyboardInterrupt:
        logger.info("Server interrupted by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
    finally:
        handler.stop_server()


if __name__ == "__main__":
    print("🚀 Starting Enhanced Message System WebSocket Server...")
    print("📝 Features enabled:")
    print("   - Priority Queue (message prioritization)")
    print("   - Batch Queue (network efficiency)")
    print("   - Retry Queue (failure handling)")
    print("🔌 Connect your frontend to ws://localhost:8000")
    
    asyncio.run(main())