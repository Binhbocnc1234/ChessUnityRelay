const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;

// Create HTTP server so we can easily bind WebSocket and add a health check endpoint
const server = http.createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('OK');
    } else {
        res.writeHead(404);
        res.end();
    }
});

const wss = new WebSocket.Server({ server });

// Active rooms map: roomCode -> { hostSocket, joinerSocket }
const rooms = new Map();

// Helper to generate a unique 4-character room code
function generateRoomCode() {
    const chars = 'ABCDEFGHIJKLMNPQRSTUVWXYZ123456789'; // Exclude ambiguous characters (O, 0)
    let code;
    do {
        code = '';
        for (let i = 0; i < 4; i++) {
            code += chars.charAt(Math.floor(Math.random() * chars.length));
        }
    } while (rooms.has(code));
    return code;
}

wss.on('connection', (ws) => {
    console.log('[Relay] New client socket connected.');
    
    // Custom properties stored on the socket instance for tracking
    ws.isAlive = true;
    ws.roomCode = null;
    ws.role = null; // 'host' or 'joiner'

    ws.on('message', (message, isBinary) => {
        if (isBinary) {
            // Relaying binary game packets (serialized MemoryPack data)
            if (!ws.roomCode) return;
            const room = rooms.get(ws.roomCode);
            if (!room) return;

            // Store action packet in the room history
            if (room.actions) {
                room.actions.push(message);
            }

            const target = ws.role === 'host' ? room.joinerSocket : room.hostSocket;
            if (target && target.readyState === WebSocket.OPEN) {
                target.send(message, { binary: true });
            }
        } else {
            // Handle control JSON text messages
            try {
                const data = JSON.parse(message.toString());
                console.log(`[Relay] Received text:`, data);

                switch (data.type) {
                    case 'create':
                        handleCreate(ws, data.roomCode);
                        break;
                    case 'join':
                        handleJoin(ws, data.roomCode);
                        break;
                    default:
                        console.warn(`[Relay] Unknown message type: ${data.type}`);
                }
            } catch (err) {
                console.error('[Relay] Error parsing JSON message:', err);
            }
        }
    });

    ws.on('close', () => {
        console.log(`[Relay] Connection closed. Room: ${ws.roomCode}, Role: ${ws.role}`);
        handleDisconnect(ws);
    });

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('error', (err) => {
        console.error('[Relay] Socket error:', err);
    });
});

function handleCreate(ws, roomCode) {
    if (ws.roomCode) {
        ws.send(JSON.stringify({ type: 'error', message: 'Already in a room' }));
        return;
    }

    // Use host's requested room code if provided, otherwise generate one
    const code = (roomCode && roomCode.trim().toUpperCase()) || generateRoomCode();
    
    if (rooms.has(code)) {
        // If it already exists, generate a unique one to avoid collision
        const newCode = generateRoomCode();
        rooms.set(newCode, {
            hostSocket: ws,
            joinerSocket: null,
            actions: []
        });
        ws.roomCode = newCode;
        ws.role = 'host';
        console.log(`[Relay] Room code collision. Created room ${newCode} instead of requested ${code}.`);
        ws.send(JSON.stringify({ type: 'created', roomCode: newCode }));
        return;
    }

    rooms.set(code, {
        hostSocket: ws,
        joinerSocket: null,
        actions: []
    });

    ws.roomCode = code;
    ws.role = 'host';

    console.log(`[Relay] Room ${code} created by Host.`);
    ws.send(JSON.stringify({ type: 'created', roomCode: code }));
}

function handleJoin(ws, roomCode) {
    if (!roomCode) {
        ws.send(JSON.stringify({ type: 'error', message: 'Room code required' }));
        return;
    }

    const code = roomCode.trim().toUpperCase();
    const room = rooms.get(code);

    if (!room) {
        console.log(`[Relay] Join rejected. Room ${code} not found.`);
        ws.send(JSON.stringify({ type: 'error', message: 'Room not found' }));
        return;
    }

    if (room.joinerSocket) {
        console.log(`[Relay] Join rejected. Room ${code} is full.`);
        ws.send(JSON.stringify({ type: 'error', message: 'Room is full' }));
        return;
    }

    // Assign joiner
    room.joinerSocket = ws;
    ws.roomCode = code;
    ws.role = 'joiner';

    console.log(`[Relay] Joiner connected to room ${code}.`);
    
    // Notify both host and joiner
    ws.send(JSON.stringify({ type: 'joined', roomCode: code }));

    // Replay historical action packets to Joiner upon connection/reconnection
    if (room.actions && room.actions.length > 0) {
        console.log(`[Relay] Replaying ${room.actions.length} historical packets to Joiner.`);
        for (const actionMessage of room.actions) {
            ws.send(actionMessage, { binary: true });
        }
    }

    if (room.hostSocket && room.hostSocket.readyState === WebSocket.OPEN) {
        room.hostSocket.send(JSON.stringify({ type: 'peer_connected' }));
    }
}

function handleDisconnect(ws) {
    if (!ws.roomCode) return;

    const room = rooms.get(ws.roomCode);
    if (!room) return;

    if (ws.role === 'host') {
        console.log(`[Relay] Host disconnected. Closing room ${ws.roomCode}.`);
        
        // Notify joiner that host disconnected and close their socket
        if (room.joinerSocket && room.joinerSocket.readyState === WebSocket.OPEN) {
            room.joinerSocket.send(JSON.stringify({ type: 'peer_disconnected', reason: 'Host disconnected' }));
            room.joinerSocket.close();
        }
        rooms.delete(ws.roomCode);
    } else if (ws.role === 'joiner') {
        console.log(`[Relay] Joiner disconnected from room ${ws.roomCode}.`);
        
        // Notify host that joiner disconnected
        if (room.hostSocket && room.hostSocket.readyState === WebSocket.OPEN) {
            room.hostSocket.send(JSON.stringify({ type: 'peer_disconnected', reason: 'Joiner disconnected' }));
        }
        room.joinerSocket = null;
    }
}

// Keep connections alive (ping/pong)
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.log('[Relay] Terminating inactive socket connection.');
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => {
    clearInterval(interval);
});

server.listen(PORT, () => {
    console.log(`[Relay] Server listening on port ${PORT}`);
});
