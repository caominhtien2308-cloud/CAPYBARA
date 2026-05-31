import asyncio
import json
import random
import logging
import os
import http
import sys
import subprocess

# Cấu hình logging ngay từ đầu để đảm bảo tất cả INFO logs được in ra lập tức
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Tự động cài đặt thư viện 'websockets' nếu môi trường chưa có sẵn (đề phòng lỗi Render Build)
try:
    import websockets
except ImportError:
    logging.warning("Không tìm thấy thư viện 'websockets'. Đang tiến hành tự động cài đặt...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
        import websockets
        logging.info("Tự động cài đặt 'websockets' thành công!")
    except Exception as e:
        logging.error(f"Lỗi khi tự động cài đặt 'websockets': {e}")
        raise e

# Monkey patch websockets to support HTTP HEAD requests from Render (which typically raise ValueError in websockets library)
try:
    import websockets.http11
    original_parse = websockets.http11.Request.parse

    @classmethod
    def patched_parse(cls, read_line):
        is_first_line = [True]
        def read_line_wrapper(*args, **kwargs):
            line = yield from read_line(*args, **kwargs)
            if is_first_line[0]:
                is_first_line[0] = False
                if line.startswith(b"HEAD "):
                    line = line.replace(b"HEAD ", b"GET ", 1)
            return line
        return (yield from original_parse(read_line_wrapper))

    websockets.http11.Request.parse = patched_parse
    logging.info("Monkey-patched websockets.http11 to support HTTP HEAD requests successfully.")
except Exception as e:
    logging.warning(f"Could not monkey-patch websockets.http11: {e}")

# Port to run on (reads from environment variables, forces 10000 on Render cloud for routing sync)
PORT = int(os.environ.get("PORT", 8765))
if os.environ.get("RENDER"):
    PORT = 10000

# Room structure:
# rooms = {
#     "ROOM_CODE": {
#         "mode": "1v1",
#         "host_id": "client_uuid",
#         "clients": {
#             "client_uuid": {
#                 "websocket": websocket,
#                 "id": "client_uuid",
#                 "name": "PlayerName",
#                 "team": "A",
#                 "ready": False,
#                 "capyIdx": 0,
#                 "wepIdx": 0,
#                 "isHost": True
#             }
#         }
#     }
# }
rooms = {}

async def broadcast_to_room(room_code, message_dict, exclude_ws=None):
    """Utility to broadcast JSON messages to all players in a room."""
    if room_code not in rooms:
        return
    message_str = json.dumps(message_dict)
    disconnected_clients = []
    
    for client_id, client in rooms[room_code]["clients"].items():
        ws = client["websocket"]
        if ws == exclude_ws:
            continue
        try:
            await ws.send(message_str)
        except Exception:
            disconnected_clients.append(client_id)
            
    # Clean up any clients that failed to receive the message
    for cid in disconnected_clients:
        await handle_disconnect_by_id(room_code, cid)

async def handle_disconnect_by_id(room_code, client_id):
    """Handles removing a client from a room and broadcasting the update."""
    if room_code not in rooms:
        return
    
    room = rooms[room_code]
    if client_id in room["clients"]:
        client_name = room["clients"][client_id]["name"]
        is_host = room["clients"][client_id]["isHost"]
        del room["clients"][client_id]
        logging.info(f"Player {client_name} ({client_id}) disconnected from room {room_code}")
        
        # If no clients left, destroy room
        if not room["clients"]:
            logging.info(f"Room {room_code} is empty. Destroying room.")
            del rooms[room_code]
            return
            
        # If the host disconnected, elect a new host
        if is_host:
            new_host_id = list(room["clients"].keys())[0]
            room["host_id"] = new_host_id
            room["clients"][new_host_id]["isHost"] = True
            room["clients"][new_host_id]["ready"] = True
            logging.info(f"Host left room {room_code}. Player {room['clients'][new_host_id]['name']} is the new Host.")
            
        # Notify remaining players
        await broadcast_to_room(room_code, {
            "type": "chat",
            "data": {
                "sender": "Hệ thống",
                "text": f"🦫 {client_name} đã thoát phòng."
            }
        })
        
        # Send room update
        await send_room_update(room_code)

async def send_room_update(room_code):
    """Sends the updated list of clients to all players in the room."""
    if room_code not in rooms:
        return
    room = rooms[room_code]
    client_list = []
    for cid, c in room["clients"].items():
        client_list.append({
            "id": c["id"],
            "name": c["name"],
            "team": c["team"],
            "ready": c["ready"],
            "capyIdx": c["capyIdx"],
            "wepIdx": c["wepIdx"],
            "isHost": c["isHost"]
        })
        
    await broadcast_to_room(room_code, {
        "type": "room-update",
        "data": {
            "code": room_code,
            "mode": room["mode"],
            "clients": client_list
        }
    })

async def handler(websocket, path=None):
    client_id = str(id(websocket))
    room_code = None
    client_name = "CapyFan"
    
    logging.info(f"New connection established: ID {client_id}")
    
    try:
        async for message in websocket:
            try:
                msg = json.loads(message)
            except json.JSONDecodeError:
                continue
                
            m_type = msg.get("type")
            m_data = msg.get("data", {})
            
            if m_type == "create":
                # Create a new room
                room_code = "".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=6))
                client_name = m_data.get("name", "HostPlayer")[:12]
                
                rooms[room_code] = {
                    "mode": m_data.get("mode", "3v3"),
                    "host_id": client_id,
                    "clients": {
                        client_id: {
                            "websocket": websocket,
                            "id": client_id,
                            "name": client_name,
                            "team": "A",
                            "ready": True,  # Host is always ready
                            "capyIdx": 0,
                            "wepIdx": -1,
                            "isHost": True
                        }
                    }
                }
                
                logging.info(f"Room {room_code} created by {client_name} ({client_id}) with mode {rooms[room_code]['mode']}")
                
                # Respond to creator
                await websocket.send(json.dumps({
                    "type": "room-created",
                    "data": {
                        "code": room_code,
                        "myId": client_id,
                        "clients": [{
                            "id": client_id,
                            "name": client_name,
                            "team": "A",
                            "ready": True,
                            "capyIdx": 0,
                            "wepIdx": -1,
                            "isHost": True
                        }]
                    }
                }))
                
            elif m_type == "join":
                # Join an existing room
                target_code = m_data.get("code", "").upper().strip()
                client_name = m_data.get("name", "GuestPlayer")[:12]
                
                if target_code not in rooms:
                    await websocket.send(json.dumps({
                        "type": "error",
                        "data": {"message": "Phòng không tồn tại hoặc đã bị hủy!"}
                    }))
                    continue
                    
                room = rooms[target_code]
                
                # Check player limits based on mode
                per_team = int(room["mode"][0]) if room["mode"] else 3
                max_players = per_team * 2
                
                if len(room["clients"]) >= max_players:
                    await websocket.send(json.dumps({
                        "type": "error",
                        "data": {"message": "Phòng đã đầy!"}
                    }))
                    continue
                
                # Set room code for cleanup reference
                room_code = target_code
                
                # Calculate balanced team assignment
                team_a_count = sum(1 for c in room["clients"].values() if c["team"] == "A")
                team_b_count = sum(1 for c in room["clients"].values() if c["team"] == "B")
                team = "B" if team_b_count < team_a_count else "A"
                
                # Register new client
                room["clients"][client_id] = {
                    "websocket": websocket,
                    "id": client_id,
                    "name": client_name,
                    "team": team,
                    "ready": False,
                    "capyIdx": 0,
                    "wepIdx": -1,
                    "isHost": False
                }
                
                logging.info(f"Player {client_name} ({client_id}) joined room {room_code}")
                
                # Confirm join to client
                client_list = []
                for cid, c in room["clients"].items():
                    client_list.append({
                        "id": c["id"],
                        "name": c["name"],
                        "team": c["team"],
                        "ready": c["ready"],
                        "capyIdx": c["capyIdx"],
                        "wepIdx": c["wepIdx"],
                        "isHost": c["isHost"]
                    })
                    
                await websocket.send(json.dumps({
                    "type": "room-joined",
                    "data": {
                        "code": room_code,
                        "myId": client_id,
                        "mode": room["mode"],
                        "clients": client_list
                    }
                }))
                
                # Broadcast chat message join
                await broadcast_to_room(room_code, {
                    "type": "chat",
                    "data": {
                        "sender": "Hệ thống",
                        "text": f"👋 {client_name} đã gia nhập sảnh!"
                    }
                })
                
                # Broadcast updated player list
                await send_room_update(room_code)
                
            elif m_type == "select":
                # User changes capy skin or weapon
                if not room_code or room_code not in rooms:
                    continue
                c = rooms[room_code]["clients"].get(client_id)
                if c:
                    c["capyIdx"] = m_data.get("capyIdx", c["capyIdx"])
                    c["wepIdx"] = m_data.get("wepIdx", c["wepIdx"])
                    await send_room_update(room_code)
                    
            elif m_type == "change-team":
                # User switches team A <-> B
                if not room_code or room_code not in rooms:
                    continue
                c = rooms[room_code]["clients"].get(client_id)
                if c:
                    c["team"] = "B" if c["team"] == "A" else "A"
                    await send_room_update(room_code)
                    
            elif m_type == "ready":
                # Player checks/unchecks ready state
                if not room_code or room_code not in rooms:
                    continue
                c = rooms[room_code]["clients"].get(client_id)
                if c and not c["isHost"]: # Host is always ready
                    c["ready"] = m_data.get("ready", not c["ready"])
                    await send_room_update(room_code)
                    
            elif m_type == "chat":
                # Normal lobby chat message
                if not room_code or room_code not in rooms:
                    continue
                await broadcast_to_room(room_code, {
                    "type": "chat",
                    "data": {
                        "sender": client_name,
                        "text": m_data.get("text", "")
                    }
                })
                
            elif m_type == "start-char-select":
                # Host triggers screen swap to character selection
                if not room_code or room_code not in rooms:
                    continue
                if rooms[room_code]["host_id"] == client_id:
                    await broadcast_to_room(room_code, {
                        "type": "start-char-select"
                    })
                    
            elif m_type == "start-game":
                # Host triggers game start
                if not room_code or room_code not in rooms:
                    continue
                if rooms[room_code]["host_id"] == client_id:
                    await broadcast_to_room(room_code, {
                        "type": "start-game"
                    })
                    
            # --- IN-GAME EVENTS ---
            elif m_type == "state":
                # Forward player coordinates to other room members
                if not room_code or room_code not in rooms:
                    continue
                m_data["id"] = client_id
                await broadcast_to_room(room_code, {
                    "type": "state-update",
                    "data": m_data
                }, exclude_ws=websocket)
                
            elif m_type == "bot-state":
                # Host updates positions of bots in the match
                if not room_code or room_code not in rooms:
                    continue
                await broadcast_to_room(room_code, {
                    "type": "bot-state-update",
                    "data": m_data
                }, exclude_ws=websocket)
                
            elif m_type == "attack":
                # Player performs a standard attack
                if not room_code or room_code not in rooms:
                    continue
                await broadcast_to_room(room_code, {
                    "type": "player-attack",
                    "data": {
                        "id": client_id,
                        "angle": m_data.get("angle", 0)
                    }
                }, exclude_ws=websocket)
                
            elif m_type == "skill":
                # Player casts a weapon skill
                if not room_code or room_code not in rooms:
                    continue
                await broadcast_to_room(room_code, {
                    "type": "player-skill",
                    "data": {
                        "id": client_id,
                        "slot": m_data.get("slot", 1),
                        "angle": m_data.get("angle", 0)
                    }
                }, exclude_ws=websocket)
                
            elif m_type == "damage":
                # Collision damage registration
                if not room_code or room_code not in rooms:
                    continue
                await broadcast_to_room(room_code, {
                    "type": "apply-damage",
                    "data": m_data
                })
                
            elif m_type == "gift-absorbed":
                # Sync custom skill gifts collected from ground
                if not room_code or room_code not in rooms:
                    continue
                await broadcast_to_room(room_code, {
                    "type": "gift-absorbed",
                    "data": m_data
                })
                
            elif m_type == "spawn-dropped-skill":
                # Sync dropped skill coordinates and types
                if not room_code or room_code not in rooms:
                    continue
                await broadcast_to_room(room_code, {
                    "type": "spawn-dropped-skill",
                    "data": m_data
                }, exclude_ws=websocket)
                
    except Exception as e:
        logging.error(f"Error handling connection {client_id}: {e}")
    finally:
        # Disconnect handling
        logging.info(f"Connection closed for ID {client_id}")
        if room_code and room_code in rooms:
            await handle_disconnect_by_id(room_code, client_id)

def health_check(*args, **kwargs):
    # Trả về HTTP 200 OK cho các yêu cầu HTTP thường (để Render Health Check thành công)
    # Hỗ trợ động cả websockets bản cũ (path, request_headers) và bản mới (connection, request)
    if len(args) >= 2:
        req_or_headers = args[1]
        if hasattr(req_or_headers, "headers"):
            headers = req_or_headers.headers
        else:
            headers = req_or_headers
    else:
        return None
        
    if "upgrade" not in headers.get("Upgrade", "").lower():
        # Hỗ trợ trả về Response object cho websockets bản mới (v11+) hoặc tuple cho các bản cũ
        try:
            import websockets.http11
            import websockets.datastructures
            return websockets.http11.Response(
                status_code=200,
                reason_phrase="OK",
                headers=websockets.datastructures.Headers([("Content-Type", "text/plain")]),
                body=b"OK"
            )
        except Exception:
            return http.HTTPStatus.OK, [("Content-Type", "text/plain")], b"OK"
    return None

async def main():
    logging.info(f"Starting CapyBrawl WebSocket Server on port {PORT}...")
    import websockets
    
    # Cài đặt máy chủ chính trên cổng PORT do Render cấu hình
    server1 = await websockets.serve(handler, "0.0.0.0", PORT, process_request=health_check)
    
    # Cài đặt máy chủ dự phòng trên cổng 10000 (Đề phòng trường hợp Render định tuyến cứng cổng 10000)
    if PORT != 10000:
        try:
            logging.info("Starting backup CapyBrawl WebSocket Server on port 10000...")
            server2 = await websockets.serve(handler, "0.0.0.0", 10000, process_request=health_check)
            logging.info("Backup server on port 10000 successfully bound and listening!")
        except Exception as e:
            logging.warning(f"Could not bind to backup port 10000: {e}")
            
    await asyncio.Event().wait()  # keep running forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Server stopped by user.")
