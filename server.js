import http from 'http';
import express from 'express';
import cors from 'cors';
import { WebSocketServer } from 'ws';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { randomUUID } from 'crypto';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

app.use(cors());
app.use(express.static(join(__dirname, 'public')));

// 루트 경로에서 game.html 제공
app.get('/', (req, res) => {
  res.sendFile(join(__dirname, 'public', 'game.html'));
});

// ===============================
// 🎮 게임 방 관리
// ===============================

const rooms = new Map();
const MAX_ROOM_SIZE = 2;

function asRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value
    : null;
}

function finite(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function sanitizePresence(value) {
  const data = asRecord(value);
  if (!data) return {};

  const presence = {};
  if (data.game === 'duel') presence.game = 'duel';
  if (finite(data.hp)) presence.hp = clamp(data.hp, 0, 100);
  if (finite(data.lx)) presence.lx = clamp(data.lx, -200, 600);
  if (finite(data.ly)) presence.ly = clamp(data.ly, -300, 1300);
  if (finite(data.al)) presence.al = clamp(data.al, 0, 1);

  if (Array.isArray(data.cast)) {
    presence.cast = data.cast
      .filter((item) => asRecord(item) !== null)
      .filter(
        (item) =>
          finite(item.i) &&
          Number.isSafeInteger(item.i) &&
          typeof item.s === 'string' &&
          /^[01]{81}$/.test(item.s) &&
          item.s.includes('1') &&
          finite(item.x) &&
          finite(item.y)
      )
      .slice(-5)
      .map((item) => ({
        i: item.i,
        s: item.s,
        x: clamp(item.x, -200, 600),
        y: clamp(item.y, -300, 1300),
      }));
  }

  if (Array.isArray(data.ult)) {
    presence.ult = data.ult
      .filter((item) => asRecord(item) !== null)
      .filter(
        (item) =>
          finite(item.i) &&
          Number.isSafeInteger(item.i) &&
          (item.k === 'cloak' || item.k === 'freeze')
      )
      .slice(-3)
      .map((item) => ({
        i: item.i,
        k: item.k,
      }));
  }
  return presence;
}

function send(socket, message) {
  if (socket.readyState === 1) {
    socket.send(JSON.stringify(message));
  }
}

function snapshot(room) {
  return [...room.members.values()].map(({ id, presence }) => ({
    peer: id,
    presence,
  }));
}

function broadcastRoom(room) {
  const message = { type: 'peers', peers: snapshot(room) };
  for (const member of room.members.values()) send(member.socket, message);
}

function removeMember(room, memberId) {
  if (!room.members.delete(memberId)) return;
  if (room.members.size === 0) {
    rooms.delete(room.name);
  } else {
    broadcastRoom(room);
  }
}

function parseMessage(data) {
  try {
    const message = asRecord(JSON.parse(data.toString('utf8')));
    if (!message || typeof message.type !== 'string') return null;
    if (
      message.type === 'join' &&
      typeof message.room === 'string' &&
      (message.mode === 'make' || message.mode === 'join')
    ) {
      return { type: 'join', room: message.room, mode: message.mode };
    }
    if (message.type === 'presence') {
      return { type: 'presence', data: message.data };
    }
    if (message.type === 'leave') return { type: 'leave' };
  } catch {
    return null;
  }
  return null;
}

function joinError(socket, code) {
  send(socket, { type: 'join-error', code });
  socket.close(1008, code);
}

// ===============================
// 🌐 WebSocket 서버
// ===============================

server.on('upgrade', (request, socket, head) => {
  let pathname = '';
  try {
    pathname = new URL(request.url ?? '/', 'http://localhost').pathname;
  } catch {
    socket.destroy();
    return;
  }
  if (pathname !== '/ws') return;
  
  wss.handleUpgrade(request, socket, head, (websocket) => {
    wss.emit('connection', websocket, request);
  });
});

wss.on('connection', (socket) => {
  let joined = null;

  socket.on('message', (raw) => {
    const message = parseMessage(raw);
    if (!message) {
      send(socket, { type: 'error', code: 'invalid-message' });
      return;
    }

    if (message.type === 'join') {
      if (joined) {
        send(socket, { type: 'error', code: 'already-joined' });
        return;
      }
      if (!/^g-[a-z0-9]{4}$/.test(message.room)) {
        joinError(socket, 'invalid-room-code');
        return;
      }

      let room = rooms.get(message.room);
      if (message.mode === 'make') {
        if (room && room.members.size > 0) {
          joinError(socket, 'room-exists');
          return;
        }
        room = { name: message.room, members: new Map() };
        rooms.set(message.room, room);
      } else if (!room || room.members.size === 0) {
        joinError(socket, 'room-not-found');
        return;
      } else if (room.members.size >= MAX_ROOM_SIZE) {
        joinError(socket, 'room-full');
        return;
      }

      const member = {
        id: randomUUID(),
        socket,
        presence: {},
        lastPresenceAt: 0,
      };
      room.members.set(member.id, member);
      joined = { room, member };
      send(socket, {
        type: 'joined',
        peer: member.id,
        room: room.name,
        peers: snapshot(room),
      });
      broadcastRoom(room);
      console.log(`✅ 플레이어 입장: ${message.room}`);
      return;
    }

    if (message.type === 'leave') {
      if (joined) removeMember(joined.room, joined.member.id);
      joined = null;
      socket.close(1000, 'left room');
      return;
    }

    if (message.type === 'presence' && joined) {
      const now = Date.now();
      const presence = sanitizePresence(message.data);
      if (now - joined.member.lastPresenceAt < 20) {
        const pendingEvents = {};
        if (presence.cast?.length) pendingEvents.cast = presence.cast;
        if (presence.ult?.length) pendingEvents.ult = presence.ult;
        if (Object.keys(pendingEvents).length > 0) {
          joined.member.presence = {
            ...joined.member.presence,
            ...pendingEvents,
          };
        }
        return;
      }
      joined.member.lastPresenceAt = now;
      joined.member.presence = {
        ...joined.member.presence,
        ...presence,
      };
      broadcastRoom(joined.room);
    }
  });

  socket.on('close', () => {
    if (joined) removeMember(joined.room, joined.member.id);
    joined = null;
  });

  socket.on('error', (error) => {
    console.error('WebSocket 에러:', error);
  });
});

// ===============================
// 🚀 서버 시작
// ===============================

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🎮 초능력 대전 서버 실행: http://localhost:${PORT}`);
  console.log(`📡 WebSocket: ws://localhost:${PORT}/ws`);
});
