"use strict";
// No database or WebRTC: rooms and actions travel over one WebSocket service.
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const PORT = Number(process.env.PORT) || 10000;
const rooms = new Map();
const MAX_PLAYERS = 20; // Host included; reconnects keep their existing seats.
const server = http.createServer((req,res)=>{
  if(req.url==='/health'||req.url==='/'){
    res.writeHead(200,{'Content-Type':'text/plain; charset=utf-8'});
    res.end('Kait multiplayer server online');
  }else{res.writeHead(404);res.end('Not found')}
});
const wss = new WebSocketServer({server,path:'/ws',maxPayload:2097152});
const send=(ws,msg)=>{if(ws?.readyState===WebSocket.OPEN)ws.send(JSON.stringify(msg))};
const err=(ws,code,message)=>send(ws,{type:'error',code,message});
const leave=ws=>{
  const r=ws.room;if(!r)return;
  if(r.host===ws){r.host=null;r.hostOfflineAt=Date.now();for(const c of r.clients.values())send(c,{type:'error',code:'host-offline',message:'Host disconnected. Waiting for reconnection…'});}
  else if(r.clients.get(ws.playerId)===ws){r.clients.delete(ws.playerId);send(r.host,{type:'offline',id:ws.playerId})}
  ws.room=null;
};
wss.on('connection',ws=>{
 ws.isAlive=true;ws.on('pong',()=>ws.isAlive=true);
 ws.on('message',raw=>{
  let m;try{m=JSON.parse(raw.toString())}catch{return}
  if(!m||typeof m!=='object')return;
  if(m.type==='create'){
    const code=String(m.code||'').toUpperCase(),token=String(m.token||'');
    if(!/^[A-Z2-9]{4}$/.test(code)||token.length<8)return err(ws,'invalid','Invalid room');
    let r=rooms.get(code);
    if(r&&(r.host||r.hostToken!==token))return err(ws,'code-taken','Room code already used');
    leave(ws);
    if(!r){r={code,hostToken:token,host:ws,clients:new Map(),lastState:null,hostOfflineAt:0};rooms.set(code,r)}
    else{r.host=ws;r.hostOfflineAt=0}
    ws.room=r;ws.role='host';send(ws,{type:'created',code});
    for(const c of r.clients.values())send(ws,{type:'hello',id:c.playerId,token:c.playerToken,chips:c.startChips});
  }else if(m.type==='join'){
    const code=String(m.code||'').toUpperCase(),r=rooms.get(code),token=String(m.token||'');
    if(!r)return err(ws,'room-not-found','Room not found. Ask host for the room code.');
    if(!r.host)return err(ws,'room-busy','Host is reconnecting. Try again shortly.');
    if(!token||token.length>100)return err(ws,'invalid','Invalid player');
    const id='p-'+token,prior=r.clients.get(id);
    // Count seats, not just active sockets: offline players retain their place.
    const known=new Set((r.lastState?.players||[]).map(p=>p.id));
    for(const connectedId of r.clients.keys())known.add(connectedId);
    known.add('h'); // One seat is reserved for the host.
    if(!known.has(id)&&known.size>=MAX_PLAYERS)
      return err(ws,'room-full','This room is full (20/20 players).');
    leave(ws);
    if(prior&&prior!==ws){prior.room=null;prior.close(1000,'Rejoined on another connection')}
    ws.room=r;ws.role='client';ws.playerId=id;ws.playerToken=token;ws.startChips=Number(m.chips)||1000;
    r.clients.set(id,ws);send(ws,{type:'joined',id});
    send(r.host,{type:'hello',id,token,chips:ws.startChips});
    if(r.lastState)send(ws,{type:'state',state:r.lastState});
  }else if(m.type==='state'&&ws.role==='host'&&ws.room?.host===ws){
    const r=ws.room;
    if(m.state?.code!==r.code||!Array.isArray(m.state?.players))return;
    r.lastState=m.state;
    for(const c of r.clients.values())send(c,{type:'state',state:m.state});
  }else if(m.type==='action'&&ws.role==='client'&&ws.room?.clients.get(ws.playerId)===ws){
    send(ws.room.host,{type:'action',id:ws.playerId,action:m.action});
  }
 });
 ws.on('close',()=>leave(ws));ws.on('error',()=>{});
});
setInterval(()=>{
 for(const ws of wss.clients){if(!ws.isAlive){ws.terminate();continue}ws.isAlive=false;ws.ping()}
 for(const [code,r] of rooms){if(!r.host&&Date.now()-r.hostOfflineAt>120000){for(const c of r.clients.values()){err(c,'host-left','Host left the room. Create a new room.');c.room=null}rooms.delete(code)}}
},25000).unref();
server.listen(PORT,'0.0.0.0',()=>console.log('Kait WebSocket multiplayer listening on',PORT));
