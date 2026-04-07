const ws = require('nodejs-websocket');

// 存储所有连接的客户端
const clients = new Set();
let heartbeatInterval = null; // 心跳定时器

const server = ws.createServer(connection => {
  console.log('建立新的WebSocket连接');
  
  // 将新连接添加到集合中
  clients.add(connection);
  console.log(`当前连接数: ${clients.size}`);
  
  // 启动心跳（如果有客户端连接）
  startHeartbeat();
  
  // 立即发送一个欢迎消息测试连接
  connection.sendText(JSON.stringify({
    type: 'welcome',
    message: '连接成功',
    timestamp: Date.now(),
    clientCount: clients.size
  }));
  
  connection.on('text', function (data) {
    console.log('收到消息:', data);
    
    // 广播给所有连接的客户端
    let sentCount = 0;
    clients.forEach(client => {
      // 检查连接状态是否为 OPEN (readyState === 1)
      if (client.readyState === 1) {
        try {
          client.sendText(data);
          sentCount++;
        } catch (err) {
          console.error(`发送失败: ${err.message}`);
        }
      }
    });
    
    console.log(`广播完成，成功发送给 ${sentCount} 个客户端，总连接数: ${clients.size}`);
  });
  
  connection.on('close', function (code, reason) {
    console.log('WebSocket连接关闭');
    clients.delete(connection);
    console.log(`当前连接数: ${clients.size}`);
    
    // 如果没有客户端了，停止心跳
    if (clients.size === 0) {
      stopHeartbeat();
    }
  });
  
  connection.on('error', (err) => {
    console.log('WebSocket错误:', err);
    clients.delete(connection);
    console.log(`当前连接数: ${clients.size}`);
    
    // 如果没有客户端了，停止心跳
    if (clients.size === 0) {
      stopHeartbeat();
    }
  });
});

// 启动心跳检测
function startHeartbeat() {
  // 如果已经有心跳定时器，先停止
  if (heartbeatInterval) {
    return;
  }
  
  console.log('检测到客户端连接，启动心跳检测');
  
  heartbeatInterval = setInterval(() => {
    if (clients.size === 0) {
      // 如果没有客户端，停止心跳
      stopHeartbeat();
      return;
    }
    
    console.log(`[心跳] 当前连接数: ${clients.size}`);
    
    // 检查每个连接的状态
    let activeCount = 0;
    let deadCount = 0;
    
    clients.forEach((client, index) => {
      if (client.readyState === 1) {
        activeCount++;
        // 发送心跳消息给客户端
        try {
          client.sendText(JSON.stringify({
            type: 'heartbeat',
            timestamp: Date.now(),
            clientCount: clients.size
          }));
        } catch (err) {
          console.error(`心跳发送失败: ${err.message}`);
          deadCount++;
        }
      } else {
        deadCount++;
        console.log(`客户端状态异常: readyState=${client.readyState}`);
      }
    });
    
    if (activeCount > 0) {
      console.log(`[心跳] 活跃连接: ${activeCount}, 异常连接: ${deadCount}`);
    }
    
    // 如果所有连接都死了，清理集合
    if (activeCount === 0 && clients.size > 0) {
      console.log('所有连接都已失效，清理连接池');
      clients.clear();
      stopHeartbeat();
    }
  }, 15000); // 每15秒发送一次心跳
}

// 停止心跳检测
function stopHeartbeat() {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
    console.log('没有客户端连接，已停止心跳检测');
  }
}

// 优雅关闭
function gracefulShutdown() {
  console.log('收到关闭信号，正在优雅关闭...');
  
  // 停止心跳
  stopHeartbeat();
  
  // 关闭所有连接
  clients.forEach(client => {
    try {
      client.close();
    } catch (err) {
      console.error('关闭连接失败:', err);
    }
  });
  
  // 关闭服务器
  server.close(() => {
    console.log('服务器已关闭');
    process.exit(0);
  });
  
  // 强制退出（如果10秒后还没关闭）
  setTimeout(() => {
    console.error('强制退出');
    process.exit(1);
  }, 10000);
}

// 监听关闭信号
process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// 启动服务器
server.listen(3000, () => {
  console.log('========================================');
  console.log('WebSocket转发服务器启动成功');
  console.log(`启动时间: ${new Date().toLocaleString()}`);
  console.log('监听端口: 3000');
  console.log('心跳策略: 有客户端连接时启动，无客户端时停止');
  console.log('========================================');
});

// 初始状态：无客户端，不启动心跳
console.log('服务器运行中，等待连接...');
console.log('当前无客户端连接，心跳未启动');